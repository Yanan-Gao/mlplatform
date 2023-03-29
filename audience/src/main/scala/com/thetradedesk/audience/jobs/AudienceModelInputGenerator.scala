package com.thetradedesk.audience.jobs

import com.amazonaws.regions.Regions
import com.amazonaws.services.s3.{AmazonS3, AmazonS3ClientBuilder}
import com.thetradedesk.audience.datasets.{AudienceModelPolicyDataset, AudienceModelPolicyRecord, CrossDeviceVendor, DataSource, Model, SeenInBiddingV3DeviceDataSet}
import com.thetradedesk.audience.sample.WeightSampling.{generatePositiveSample, generateNegativeSample, getLabels, zipAndGroupUDF}
import com.thetradedesk.audience.{date, shouldConsiderTDID2}
import com.thetradedesk.geronimo.bidsimpression.schema.{BidsImpressions, BidsImpressionsSchema}
import com.thetradedesk.geronimo.shared.{GERONIMO_DATA_SOURCE, loadParquetData}
import com.thetradedesk.spark.TTDSparkContext.spark
import com.thetradedesk.spark.TTDSparkContext.spark.implicits._
import com.thetradedesk.spark.util.TTDConfig.config
import com.thetradedesk.spark.util.prometheus.PrometheusClient
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{DataFrame, Dataset}

import java.time.LocalDate
import java.time.format.DateTimeFormatter

object AudienceModelInputGeneratorJob {
  object Config {
    val modelId = config.getIntRequired("modelId")

    val supportedDataSources = config.getStringRequired("supportedDataSources").split(',')
      .map(dataSource => dataSource.toInt)

    val seedToSplitDataset = config.getStringRequired("seedToSplitDataset")

    val validateDatasetSplitModule = config.getInt("validateDatasetSplitModule", default = 5)
  }

  def main(args: Array[String]): Unit = {
    def runETLPipeline(): Unit = {
      val policyTable = clusterTargetingData()

      policyTable.foreach(typePolicyTable => {
        val dataset = typePolicyTable match {
          case ((Model.AEM, DataSource.SIB, CrossDeviceVendor.None), subPolicyTable: Array[AudienceModelPolicyRecord]) =>
            FirstPartyPixelSIBModelInputGenerator.generateDataset(date, subPolicyTable)
          case ((Model.RSM, DataSource.Seed, CrossDeviceVendor.None), subPolicyTable: Array[AudienceModelPolicyRecord]) =>
            SeedDailyModelInputGenerator.generateDataset(date, subPolicyTable)
          case _ =>
            // todo make this better
            throw new Exception("error")
        }

        dataset.withColumn("isValidate", hash(concat('TDID, lit(Config.seedToSplitDataset))) % Config.validateDatasetSplitModule === lit(0))
      })
    }

    def clusterTargetingData(): Map[(Int, Int, Int), Array[AudienceModelPolicyRecord]] = {
      val policyTable = AudienceModelPolicyDataset()
        .readPartition(date)(spark)
        .where('Model === lit(Config.modelId))
        .where('Source isin Config.supportedDataSources)
        .collect()

      // todo group by record by targeting data size
      policyTable.groupBy(e => (e.Model, e.Source, e.CrossDeviceVendorId))
    }
  }
}

/**
 * This is the base class for audience model training data generation
 * including AEM(audience extension model), RSM(relevance score model), etc
 */
abstract class AudienceModelInputGenerator {

  val samplingFunction = shouldConsiderTDID2 _
  val prometheus = new PrometheusClient("AudienceModel", this.getClass.getSimpleName)

  /**
   * Common configurations could be put here
   */
  object Config {
    val numTDID = config.getInt("numTDID", 100)

    val labelLookBack = config.getInt("labelLookBack", 0)

    val selectedPixelsConfigPath = config.getString("selectedPixelsConfigPath", "s3a://thetradedesk-useast-hadoop/Data_Science/freeman/audience_extension/firstPixel46_TargetingDataId/")

    // detect recent seed raw data path in airflow and pass to spark job
    val seedRawDataPath = config.getString("seedRawDataPath", "")

    // detect recent seed metadata path in airflow and pass to spark job
    val seedMetadataPath = config.getString("seedMetadataPath", "")

    val lastTouchNumberInBR = config.getInt("lastTouchNumberInBR", 3)

    val positiveSampleUpperThreshold = config.getDouble("positiveSampleUpperThreshold", default = 20000.0)

    val positiveSampleLowerThreshold = config.getDouble("positiveSampleLowerThreshold", default = 2000.0)
    
    val positiveSampleSmoothingFactor = config.getDouble("positiveSampleSmoothingFactor", default = 0.95)
    
    val negativeSampleRatio = config.getInt("negativeSampleRatio", default = 5)
    
    val labelMaxLength = config.getInt("labelMaxLength", default = 50)
  }

  /**
   * Core logic to generate model training dataset should be put here
   */
  def generateDataset(date: LocalDate, policyTable: Array[AudienceModelPolicyRecord]): DataFrame

  def generateLabels(date: LocalDate, policyTable: Broadcast[Array[AudienceModelPolicyRecord]]): DataFrame

  def getBidImpressions(date: LocalDate) = {
    val bidImpressionsS3Path = BidsImpressions.BIDSIMPRESSIONSS3 + "prod/bidsimpressions/"

    val bidsImpressionsLong = loadParquetData[BidsImpressionsSchema](bidImpressionsS3Path, date.minusDays(Config.labelLookBack), source = Some(GERONIMO_DATA_SOURCE))
      .withColumnRenamed("UIID", "TDID")
      .filter(samplingFunction('TDID))
      .select('BidRequestId, // use to connect with bidrequest, to get more features
        'AdvertiserId,
        'AdGroupId,
        'SupplyVendor,
        'DealId,
        'SupplyVendorPublisherId,
        'Site,
        'AdWidthInPixels,
        'AdHeightInPixels,
        'Country,
        'Region,
        'City,
        'Zip, // cast to first three digits for US is enough
        'DeviceMake,
        'DeviceModel,
        'RequestLanguages,
        'RenderingContext,
        'DeviceType,
        'OperatingSystemFamily,
        'Browser,
        'sin_hour_week, // time based features sometime are useful than expected
        'cos_hour_week,
        'sin_hour_day,
        'cos_hour_day,
        'Latitude,
        'Longitude
      )
      // they saved in struct type
      .withColumn("OperatingSystemFamily", 'OperatingSystemFamily("value"))
      .withColumn("Browser", 'Browser("value"))
      .withColumn("RenderingContext", 'RenderingContext("value"))
      .withColumn("DeviceType", 'DeviceType("value"))
      .withColumn("AdWidthInPixels", ('AdWidthInPixels - lit(1.0)) / lit(9999.0)) // 1 - 10000
      .withColumn("AdWidthInPixels", when('AdWidthInPixels.isNotNull, 'AdWidthInPixels).otherwise(0))
      .withColumn("AdHeightInPixels", ('AdHeightInPixels - lit(1.0)) / lit(9999.0)) // 1 - 10000
      .withColumn("AdHeightInPixels", when('AdHeightInPixels.isNotNull, 'AdHeightInPixels).otherwise(0))
      .withColumn("Latitude", ('Latitude + lit(90.0)) / lit(180.0)) // -90 - 90
      .withColumn("Latitude", when('Latitude.isNotNull, 'Latitude).otherwise(0))
      .withColumn("Longitude", ('Longitude + lit(180.0)) / lit(360.0)) //-180 - 180
      .withColumn("Longitude", when('Longitude.isNotNull, 'Longitude).otherwise(0))
      .cache()

    val sampledBidsImpressionsKeys = loadParquetData[BidsImpressionsSchema](bidImpressionsS3Path, date.minusDays(Config.labelLookBack), source = Some(GERONIMO_DATA_SOURCE))
      .withColumnRenamed("UIID", "TDID")
      .select('BidRequestId, 'TDID, 'CampaignId, 'LogEntryTime)
      .filter(samplingFunction('TDID)) // in the future, we may not have the id, good to think about how to solve
      .cache()

    (ApplyNLastTouchOnSameTdid(sampledBidsImpressionsKeys), bidsImpressionsLong)
  }

  def sampleLabels(labels: DataFrame, policyTable: Array[AudienceModelPolicyRecord], targetName:String): DataFrame = {
    
    // Aggregate policy table as an one row
    val policyTableDF = policyTable.toSeq.toDF()
    val aggregatedPolicy = policyTableDF.agg(collect_list(struct(col(targetName), col("OrderId"), col("size")))
                                      .alias("id_size_pairs"))
                                      .selectExpr(s"transform(id_size_pairs, x -> x.${targetName}) as id", "transform(id_size_pairs, x -> x.size) as size", "transform(id_size_pairs, x -> x.OrderId) as OrderId")
                                      .cache()
    
    val aggregatedPolicyAboveThreshold = policyTableDF.filter(col("size")>Config.positiveSampleLowerThreshold)
                                      .withColumn("adjustedSize", when(col("size")>Config.positiveSampleUpperThreshold, Config.positiveSampleUpperThreshold).otherwise(col("size")))
                                      .agg(collect_list(struct(col(targetName), col("OrderId"), col("size"), col("adjustedSize")))
                                      .alias("id_size_pairs"))
                                      .selectExpr(s"transform(id_size_pairs, x -> x.${targetName}) as AboveThresholdId", "transform(id_size_pairs, x -> x.size) as AboveThresholdSize", "transform(id_size_pairs, x -> x.adjustedSize) as adjustedAboveThresholdSize", "transform(id_size_pairs, x -> x.OrderId) as adjustedAboveThresholdOrderId")
                                      .cache()

    // calculate the 95 percentile of length of the id array column
    val label_95_pct = labels.withColumn("size", size(col(s"${targetName}s")))
                              .agg(percentile_approx('size, lit(0.95),lit(10000000)))
                              .head()
                              .getInt(0)
                              .toDouble

    // repartition labels table to larger size for better parallel computing
    val labels_repartitioned = labels.repartition(500)
//                                   .filter(size(col(s"${targetName}s"))<=label_95_pct)
                                  .cache()

    val labels_overall_size = labels_repartitioned.count()
    
    // downsample positive labels to keep # of positive labels among targets balanced 
    val positiveLabelResult = labels_repartitioned.select(col("TDID"), col(s"${targetName}s"))
                          .crossJoin(aggregatedPolicy)
                          .crossJoin(aggregatedPolicyAboveThreshold)
                          .select(col("TDID"), col(s"${targetName}s"), col("id"), col("OrderId"), col("size"), col("AboveThresholdId"), col("AboveThresholdSize"), col("adjustedAboveThresholdSize"), col("adjustedAboveThresholdOrderId"))
                          .withColumn("positiveResults", generatePositiveSample(col(s"${targetName}s")
                                                                              , col("id")
                                                                              , col("OrderId")
                                                                              , col("size")
                                                                              , lit(Config.positiveSampleUpperThreshold)
                                                                              , lit(Config.positiveSampleLowerThreshold)
                                                                              , lit(Config.positiveSampleSmoothingFactor)
                                                                              , lit(labels_overall_size)))
                          .withColumn("positiveSamples", col("positiveResults._1"))
                          .withColumn("positiveOrderIds", col("positiveResults._2"))
                          
                          .cache()
    
    // sample negative labels to match the pos:neg ratio per targets
    val labelResult = positiveLabelResult.withColumn("negativeResults", generateNegativeSample(col("AboveThresholdId")
                                                                              , col("adjustedAboveThresholdOrderId")
                                                                              , col("AboveThresholdSize")
                                                                              , col("adjustedAboveThresholdSize") 
                                                                              , lit(Config.positiveSampleUpperThreshold)
                                                                              , lit(Config.positiveSampleLowerThreshold)
                                                                              , lit(labels_overall_size)
                                                                              , lit(Config.negativeSampleRatio)*size(col("positiveSamples"))))
                          .withColumn("negativeSamples", col("negativeResults._1"))
                          .withColumn("negativeOrderIds", col("negativeResults._2"))
                          .withColumn("negativeSamples", array_except(col("negativeSamples"),col(s"${targetName}s")))
                          .withColumn("positiveTargets", getLabels($"positiveSamples",lit(1)))
                          .withColumn("negativeTargets", getLabels($"negativeSamples",lit(0)))
                          .withColumn(s"${targetName}s", concat($"positiveSamples",$"negativeSamples"))
                          .withColumn("targets", concat($"positiveTargets",$"negativeTargets"))
                          .select(col("TDID"), col(s"${targetName}s"), col("targets"))
                          // partialy explode the result to keep the target array within the max length
                          .withColumn("zipped_targets", zipAndGroupUDF(col(s"${targetName}s"), col("targets"), lit(Config.labelMaxLength)))
                          .select(col("TDID"),explode(col("zipped_targets")).as("zipped_targets"))
                          .select(col("TDID"), col("zipped_targets").getField("_1").as(s"${targetName}s"), col("zipped_targets").getField("_2").as("targets"))

    labelResult
  }

  private def ApplyNLastTouchOnSameTdid(sampledBidsImpressionsKeys: DataFrame) = {
    val window = Window.partitionBy('TDID).orderBy('LogEntryTime.desc)
    sampledBidsImpressionsKeys.withColumn("row", row_number().over(window))
      .filter('row <= Config.lastTouchNumberInBR)
      .drop('LogEntryTime)
      .drop('row)
  }

}

/**
 * This class is used to generate model training samples for first party pixel model
 * using seenInBidding dataset
 */
object FirstPartyPixelSIBModelInputGenerator extends AudienceModelInputGenerator {

  def generateDataset(date: LocalDate, policyTable: Array[AudienceModelPolicyRecord]): DataFrame = {
    val (sampledBidsImpressionsKeys, bidsImpressionsLong) = getBidImpressions(date)
    val labels = generateLabels(date, spark.sparkContext.broadcast(policyTable))

    // TODO dedup sampledBidsImpressionsKeys in case huge impressions for same TDID
    val roughDataset = sampledBidsImpressionsKeys
      .join(
        sampleLabels(labels, policyTable, "TargetingDataId"), Seq("TDID"), "inner")
      .join(bidsImpressionsLong, Seq("BidRequestId"), "inner")

    // TODO refine/re-sample dataset
    val refinedDataset = roughDataset

    refinedDataset
  }


  def refineLabels(labels: DataFrame): DataFrame = {
    /* TODO add weighted downSample logic to refine positive label size and negative label size
     *   https://atlassian.thetradedesk.com/confluence/display/EN/ETL+and+model+training+pipline+based+on+SIB+dataset
     */
    labels
  }

  def generateLabels(date: LocalDate, policyTable: Broadcast[Array[AudienceModelPolicyRecord]]): DataFrame = {
    val targetingDataIds = policyTable.value.map(
      record => record.TargetingDataId
    ).toSeq
    // FIXME use cross device SIB dataset to replace this one
    SeenInBiddingV3DeviceDataSet().readPartition(date, lookBack = Some(Config.labelLookBack))(spark)
      .withColumnRenamed("DeviceId", "TDID")
      .filter(samplingFunction('TDID))
      // only support first party targeting data ids in current solution
      .withColumn("TargetingDataIds", array_intersect('FirstPartyTargetingDataIds, typedLit(targetingDataIds)))
      .filter(size(col("TargetingDataIds"))>0)
      .select('TDID, 'TargetingDataIds)
  }
}

/**
 * This class is used to generate model training samples for relevance score model
 * using the dataset provided by seed service
 */
object SeedDailyModelInputGenerator extends AudienceModelInputGenerator {
  override def generateDataset(date: LocalDate, policyTable: Array[AudienceModelPolicyRecord]): DataFrame = {
    val seedDataset = generateLabels(date, spark.sparkContext.broadcast(policyTable))
    seedDataset
  }

  /**
   * read seed data from s3
   * seed data should be SeedId to TDID
   *
   * @param date
   * @param seedIds
   * @return
   */
  def generateLabels(date: LocalDate, policyTable: Broadcast[Array[AudienceModelPolicyRecord]]): DataFrame = {
    policyTable.value.map(
      record => {
        /**
         * seed data is stored in the following format, we want to use the recent date to process
         * seedRawDataPath
         *   - seedA
         *     - _CURRENT
         *     - 20230211
         *     - 20230219
         *     - 20230312
         *   - seedB
         *     - _CURRENT
         *     - 20230217
         *     - 20230318
         *
         * @return
         */
        val seedDataPath = Config.seedRawDataPath + "/" + record.SourceId
        // todo solve the problem when the current seed is not updated in s3
        val recentVersionBeforeDate = spark
          .sparkContext
          .textFile(seedDataPath + "/_CURRENT")
          .collect()
          .apply(0)

        spark.read.parquet(seedDataPath + "/" + recentVersionBeforeDate)
          .withColumn("orderId", lit(record.OrderId))
          .select('TDID, 'orderId)
      }
    ).reduce(_ unionAll _)
  }
}