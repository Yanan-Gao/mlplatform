package com.thetradedesk.audience.jobs

import com.thetradedesk.audience.datasets._
import com.thetradedesk.audience.sample.WeightSampling.{getLabels, negativeSampleUDFGenerator, positiveSampleUDFGenerator, zipAndGroupUDFGenerator}
import com.thetradedesk.audience.transform.ModelFeatureTransform
import com.thetradedesk.audience.{date, shouldConsiderTDID3}
import com.thetradedesk.geronimo.bidsimpression.schema.{BidsImpressions, BidsImpressionsSchema}
import com.thetradedesk.geronimo.shared.{GERONIMO_DATA_SOURCE, loadParquetData}
import com.thetradedesk.spark.TTDSparkContext.spark
import com.thetradedesk.spark.TTDSparkContext.spark.implicits._
import com.thetradedesk.spark.util.TTDConfig.{config, defaultCloudProvider}
import com.thetradedesk.spark.util.prometheus.PrometheusClient
import org.apache.spark.sql.{DataFrame, Row, SaveMode}
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.{IntegerType, StringType, StructField, StructType}

import java.time.LocalDate
import java.time.format.DateTimeFormatter

object AudienceModelInputGeneratorJob {
  object Config {
    val model = Model.withName(config.getStringRequired("modelName"))

    val supportedDataSources = config.getStringRequired("supportedDataSources").split(',')
      .map(dataSource => DataSource.withName(dataSource).id)

    val seedToSplitDataset = config.getStringRequired("seedToSplitDataset")

    val validateDatasetSplitModule = config.getInt("validateDatasetSplitModule", default = 5)

    var subFolder = config.getString("subFolder", "split")

    var persistHoldoutSet = config.getBoolean("persistHoldoutSet", default = false)
  }

  object SubFolder extends Enumeration {
    type SubFolder = Value
    val Val, Holdout, Train = Value
  }

  def main(args: Array[String]): Unit = {
    runETLPipeline()
  }

  def runETLPipeline(): Unit = {
    val policyTable = clusterTargetingData()

    policyTable.foreach(typePolicyTable => {
      val dataset = {
        Config.model match {
          case Model.AEM =>
            typePolicyTable match {
              case ((DataSource.SIB, CrossDeviceVendor.None), subPolicyTable: Array[AudienceModelPolicyRecord]) =>
                AEMSIBInputGenerator.generateDataset(date, subPolicyTable)
              case ((DataSource.Conversion, CrossDeviceVendor.None), subPolicyTable: Array[AudienceModelPolicyRecord]) =>
                AEMConversionInputGenerator.generateDataset(date, subPolicyTable)
              case _ => throw new Exception(s"unsupported policy settings: Model[${Model.AEM}], Setting[${typePolicyTable._1}]")
            }
          case Model.RSM =>
            typePolicyTable match {
              case ((DataSource.Seed, CrossDeviceVendor.None), subPolicyTable: Array[AudienceModelPolicyRecord]) =>
                RSMSeedInputGenerator.generateDataset(date, subPolicyTable)
              case _ => throw new Exception(s"unsupported policy settings: Model[${Model.RSM}], Setting[${typePolicyTable._1}]")
            }
          case _ => throw new Exception(s"unsupported Model[${Config.model}]")
        }
      }

      val resultSet = ModelFeatureTransform.modelFeatureTransform[AudienceModelInputRecord](dataset)
        .withColumn("SplitRemainder", hash(concat('TDID, lit(Config.seedToSplitDataset))) % Config.validateDatasetSplitModule)
        .withColumn("SubFolder",
          when('SplitRemainder === lit(SubFolder.Val.id), SubFolder.Val.toString)
            .when('SplitRemainder === lit(SubFolder.Holdout.id), SubFolder.Holdout.toString)
            .otherwise(SubFolder.Train.toString))

      resultSet.cache()

      AudienceModelInputDataset(Config.model.toString, s"${typePolicyTable._1._1}_${typePolicyTable._1._2}").writePartition(
        resultSet.filter('SubFolder === lit(SubFolder.Val.id)).as[AudienceModelInputRecord],
        date,
        subFolderKey = Some(Config.subFolder),
        subFolderValue = Some(SubFolder.Val.toString),
        format = Some("tfrecord"),
        saveMode = SaveMode.Overwrite
      )

      if (Config.persistHoldoutSet) {
        AudienceModelInputDataset(Config.model.toString, s"${typePolicyTable._1._1}_${typePolicyTable._1._2}").writePartition(
          resultSet.filter('SubFolder === lit(SubFolder.Holdout.id)).as[AudienceModelInputRecord],
          date,
          subFolderKey = Some(Config.subFolder),
          subFolderValue = Some(SubFolder.Holdout.toString),
          format = Some("tfrecord"),
          saveMode = SaveMode.Overwrite
        )
      }

      AudienceModelInputDataset(Config.model.toString, s"${typePolicyTable._1._1}_${typePolicyTable._1._2}").writePartition(
        resultSet.filter('SubFolder === lit(SubFolder.Train.id)).as[AudienceModelInputRecord],
        date,
        subFolderKey = Some(Config.subFolder),
        subFolderValue = Some(SubFolder.Train.toString),
        format = Some("tfrecord"),
        saveMode = SaveMode.Overwrite
      )

      resultSet.unpersist()
    }
    )
  }

  def clusterTargetingData(): Map[(DataSource.DataSource, CrossDeviceVendor.CrossDeviceVendor), Array[AudienceModelPolicyRecord]] = {
    val policyTable = AudienceModelPolicyDataset(Config.model)
      .readPartition(date)(spark)
      .where('IsActive)
      .where('Source.isin(Config.supportedDataSources: _*))
      .collect()

    // todo group records by targeting data size
    policyTable.groupBy(e => (DataSource(e.Source), CrossDeviceVendor(e.CrossDeviceVendorId)))
  }
}

/**
 * This is the base class for audience model training data generation
 * including AEM(audience extension model), RSM(relevance score model), etc
 */
abstract class AudienceModelInputGenerator(name: String) {

  val samplingFunction = shouldConsiderTDID3(_, config.getInt(s"userDownSampleHitPopulation${name}", default = 1000000))
  val prometheus = new PrometheusClient("AudienceModel", name)
  val mappingFunctionGenerator =
    (dictionary: Map[Any, Int]) =>
      udf((values: Array[Any]) =>
        values.filter(dictionary.contains).map(dictionary.getOrElse(_, -1)))

  /**
   * Common configurations could be put here
   */
  object Config {
    val numTDID = config.getInt("numTDID", 100)

    val bidImpressionLookBack = config.getInt("bidImpressionLookBack", 0)

    val seenInBiddingLookBack = config.getInt("seenInBiddingLookBack", 0)

    // detect recent seed raw data path in airflow and pass to spark job
    val seedRawDataPath = config.getString("seedRawDataPath", "")

    // detect recent seed metadata path in airflow and pass to spark job
    val seedMetadataPath = config.getString("seedMetadataPath", "")

    // last n bid impressions we care about
    val lastTouchNumberInBR = config.getInt("lastTouchNumberInBR", 3)

    // conversion data look back days
    val conversionLookBack = config.getInt("conversionLookBack", 1)

    // default value of minimal positive label size in SIB dataset
    val minimalPositiveLabelSizeOfSIB = config.getInt("minimalPositiveLabelSizeOfSIB", 0)

    // todo merge threshold settings into policy table
    val positiveSampleUpperThreshold = config.getDouble("positiveSampleUpperThreshold", default = 20000.0)

    val positiveSampleLowerThreshold = config.getDouble("positiveSampleLowerThreshold", default = 2000.0)

    val positiveSampleSmoothingFactor = config.getDouble("positiveSampleSmoothingFactor", default = 0.95)

    val negativeSampleRatio = config.getInt("negativeSampleRatio", default = 5)

    val labelMaxLength = config.getInt("labelMaxLength", default = 50)

    val recordIntermediateResult = config.getBoolean("recordIntermediateResult", default = false)
  }

  /**
   * Core logic to generate model training dataset should be put here
   */
  def generateDataset(date: LocalDate, policyTable: Array[AudienceModelPolicyRecord]): DataFrame = {
    val (sampledBidsImpressionsKeys, bidsImpressionsLong, uniqueTDIDs) = getBidImpressions(date)


    val rawLabels = generateLabels(date, policyTable)

    val filteredLabels = rawLabels.join(uniqueTDIDs, Seq("TDID"), "inner")

    if (Config.recordIntermediateResult) {
      filteredLabels.write.parquet(s"s3a://thetradedesk-mlplatform-us-east-1/data/dev/audience/firstPartyPixel/uniEtlTestIntermediate/${name}/date=${date.format(DateTimeFormatter.BASIC_ISO_DATE)}")
    }

    val refinedLabels = sampleLabels(filteredLabels, policyTable)

    val roughResult = bidsImpressionsLong
      .drop("TDID", "CampaignId", "LogEntryTime")
      .join(
        refinedLabels
          .join(
            sampledBidsImpressionsKeys, Seq("TDID"), "inner"),
        Seq("BidRequestId"), "inner")

    refineResult(roughResult)
  }

  def refineResult(roughResult: DataFrame): DataFrame = {
    /* TODO add weighted downSample logic to refine positive label size and negative label size
     *   https://atlassian.thetradedesk.com/confluence/display/EN/ETL+and+model+training+pipline+based+on+SIB+dataset
     */
    roughResult
  }

  def generateLabels(date: LocalDate, policyTable: Array[AudienceModelPolicyRecord]): DataFrame

  def getBidImpressions(date: LocalDate) = {
    val bidImpressionsS3Path = BidsImpressions.BIDSIMPRESSIONSS3 + "prod/bidsimpressions/"

    val bidsImpressionsLong = loadParquetData[BidsImpressionsSchema](bidImpressionsS3Path, date.minusDays(Config.bidImpressionLookBack), source = Some(GERONIMO_DATA_SOURCE))
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
        'Longitude,
        'MatchedFoldPosition,
        'InternetConnectionType,
        'OperatingSystem,
        'sin_minute_hour,
        'cos_minute_hour,
        'sin_minute_day,
        'cos_minute_day,
        'CampaignId,
        'TDID,
        'LogEntryTime
      )
      // they saved in struct type
      .withColumn("OperatingSystemFamily", 'OperatingSystemFamily("value"))
      .withColumn("Browser", 'Browser("value"))
      .withColumn("RenderingContext", 'RenderingContext("value"))
      .withColumn("InternetConnectionType", 'InternetConnectionType("value"))
      .withColumn("OperatingSystem", 'OperatingSystem("value"))
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

    val sampledBidsImpressionsKeys = ApplyNLastTouchOnSameTdid(
      bidsImpressionsLong
        .select('BidRequestId, 'TDID, 'CampaignId, 'LogEntryTime))
      .cache()

    val uniqueTDIDs = sampledBidsImpressionsKeys.select('TDID).distinct().cache()

    (sampledBidsImpressionsKeys, bidsImpressionsLong, uniqueTDIDs)
  }

  private def ApplyNLastTouchOnSameTdid(sampledBidsImpressionsKeys: DataFrame) = {
    val window = Window.partitionBy('TDID).orderBy('LogEntryTime.desc)
    sampledBidsImpressionsKeys.withColumn("row", row_number().over(window))
      .filter('row <= Config.lastTouchNumberInBR)
      .drop('LogEntryTime)
      .drop('row)
      .cache()
  }

  /** https://atlassian.thetradedesk.com/confluence/display/EN/ETL+and+model+training+pipline+based+on+SIB+dataset
   * https://atlassian.thetradedesk.com/confluence/display/EN/RSM+-+Weight+Sampling+Labels
   * */
  private def sampleLabels(labels: DataFrame, policyTable: Array[AudienceModelPolicyRecord]): DataFrame = {
    // calculate the 95 percentile of length of the id array column
    //    val label_95_pct = labels.withColumn("size", size('))
    //      .agg(percentile_approx('size, lit(0.95), lit(10000000)))
    //      .head()
    //      .getInt(0)
    //      .toDouble

    val syntheticIdToPolicy = policyTable
      .map(e => (e.SyntheticId, e))
      .toMap

    val positiveSampleUDF = positiveSampleUDFGenerator(
      syntheticIdToPolicy,
      Config.positiveSampleUpperThreshold,
      Config.positiveSampleLowerThreshold,
      Config.positiveSampleSmoothingFactor)

    val labelDatasetSize = labels.count().toInt

    val aboveThresholdPolicyTable = policyTable
      .filter(e => e.Size >= Config.positiveSampleLowerThreshold)

    val negativeSampleUDF = negativeSampleUDFGenerator(
      aboveThresholdPolicyTable,
      Config.positiveSampleUpperThreshold,
      labelDatasetSize)

    // downsample positive labels to keep # of positive labels among targets balanced
    val labelResult = labels
      .withColumn("PositiveSamples", positiveSampleUDF('PositiveSyntheticIds))
      .withColumn("NegativeSamples", negativeSampleUDF(lit(Config.negativeSampleRatio) * size(col("PositiveSamples"))))
      .withColumn("NegativeSamples", array_except(col("NegativeSamples"), 'PositiveSyntheticIds))
      .withColumn("PositiveTargets", getLabels($"PositiveSamples", lit(1)))
      .withColumn("NegativeTargets", getLabels($"NegativeSamples", lit(0)))
      .withColumn("Labels", concat($"PositiveSamples", $"NegativeSamples"))
      .withColumn("Targets", concat($"PositiveTargets", $"NegativeTargets"))
      .select('TDID, 'Labels, 'Targets)
      // partialy explode the result to keep the target array within the max length
      .withColumn("ZippedTargets", zipAndGroupUDFGenerator(Config.labelMaxLength)('Labels, 'Targets))
      .select(col("TDID"), explode(col("ZippedTargets")).as("ZippedTargets"))
      .select(col("TDID"), col("ZippedTargets").getField("_1").as("Labels"), col("ZippedTargets").getField("_2").as("Targets"))

    labelResult
  }
}

/**
 * This class is used to generate model training samples for first party pixel model
 * using seenInBidding dataset
 */
object AEMSIBInputGenerator extends AudienceModelInputGenerator("AEMSIB") {

  override def generateLabels(date: LocalDate, policyTable: Array[AudienceModelPolicyRecord]): DataFrame = {
    val mappingFunction = mappingFunctionGenerator(
      policyTable.map(e => (e.TargetingDataId, e.SyntheticId)).toMap)

    // FIXME use cross device SIB dataset to replace this one
    SeenInBiddingV3DeviceDataSet().readPartition(date, lookBack = Some(Config.seenInBiddingLookBack))(spark)
      .withColumnRenamed("DeviceId", "TDID")
      .filter(samplingFunction('TDID))
      // only support first party targeting data ids in current solution
      .withColumn("PositiveSyntheticIds", mappingFunction('FirstPartyTargetingDataIds))
      .filter(size('PositiveSyntheticIds) > Config.minimalPositiveLabelSizeOfSIB)
      .select('TDID, 'PositiveSyntheticIds)
      .cache()
  }
}

/**
 * This class is used to generate model training samples for audience extension model
 * using conversion tracker dataset
 */
object AEMConversionInputGenerator extends AudienceModelInputGenerator("AEMConversion") {
  private val mappingSchema = StructType(
    Array(
      StructField("TrackingTagId", StringType, nullable = false),
      StructField("SyntheticId", IntegerType, nullable = false)
    ))


  override def generateLabels(date: LocalDate, policyTable: Array[AudienceModelPolicyRecord]): DataFrame = {
    val mappingRows = policyTable.
      map(e => Row(e.SourceId, e.SyntheticId))
      .toSeq

    val mappingDataset = broadcast(
      spark.createDataFrame(
        spark.sparkContext.parallelize(mappingRows),
        mappingSchema))

    ConversionDataset(defaultCloudProvider)
      .readRange(date.minusDays(Config.conversionLookBack).atStartOfDay(), date.atStartOfDay())
      .select('TDID, 'TrackingTagId)
      .filter(samplingFunction('TDID))
      .join(mappingDataset, "TrackingTagId")
      .groupBy('TDID)
      .agg(collect_set('SyntheticId) as "PositiveSyntheticIds")
      .cache()
  }
}

/**
 * This class is used to generate model training samples for relevance score model
 * using the dataset provided by seed service
 */
object RSMSeedInputGenerator extends AudienceModelInputGenerator("RSMSeed") {

  /**
   * read seed data from s3
   * seed data should be SeedId to TDID
   *
   * @param date
   * @param policyTable
   * @return
   */
  override def generateLabels(date: LocalDate, policyTable: Array[AudienceModelPolicyRecord]): DataFrame = {
    policyTable.map(
      record => {
        /**
         * seed data is stored in the following format, we want to use the recent date to process
         * seedRawDataPath
         *   - seedA
         *     - _CURRENT (20230312)
         *     - 20230211
         *     - 20230219
         *     - 20230312
         *   - seedB
         *     - _CURRENT (20230318)
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
          .trim

        spark.read.parquet(seedDataPath + "/" + recentVersionBeforeDate)
          .filter(samplingFunction('TDID))
          .withColumn("SyntheticId", lit(record.SyntheticId))
          .select('TDID, 'SyntheticId)
      }
    ).reduce(_ unionAll _)
      .groupBy('TDID)
      .agg(collect_list('SyntheticId) as "PositiveSyntheticIds")
      .cache()
  }
}