package com.thetradedesk.audience.jobs

import com.thetradedesk.audience.datasets._
import com.thetradedesk.audience.sample.WeightSampling.{getLabels, negativeSampleUDFGenerator, positiveSampleUDFGenerator, zipAndGroupUDFGenerator}
import com.thetradedesk.audience.transform.ModelFeatureTransform
import com.thetradedesk.audience.utils.S3Utils
import com.thetradedesk.audience.{dateTime, shouldConsiderTDID3, ttdEnv}
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

object audienceModelInputConfig {
  // ********************* used for generator job *********************
    // val model = Model.withName(config.getStringRequired("modelName"))

    // val supportedDataSources = config.getStringRequired("supportedDataSources").split(',')
    //   .map(dataSource => DataSource.withName(dataSource).id)

    // val saltToSplitDataset = config.getStringRequired("saltToSplitDataset")

    val model = Model.withName(config.getString("modelName", default = "RSM"))

    val supportedDataSources = config.getString("supportedDataSources", default = "Seed").split(',')
      .map(dataSource => DataSource.withName(dataSource).id)

    val saltToSplitDataset = config.getString("saltToSplitDataset", default = "RSMSplit")

    val validateDatasetSplitModule = config.getInt("validateDatasetSplitModule", default = 5)

    var subFolder = config.getString("subFolder", "split")

    var persistHoldoutSet = config.getBoolean("persistHoldoutSet", default = false)

    var seedSizeLowerScaleThreshold = config.getInt("seedSizeLowerScaleThreshold", default = 1)

    var seedSizeUpperScaleThreshold = config.getInt("seedSizeUpperScaleThreshold", default = 12)

    // ***************** used for model input generator *********************
    val numTDID = config.getInt("numTDID", 100)

    val bidImpressionLookBack = config.getInt("bidImpressionLookBack", 1)

    val seenInBiddingLookBack = config.getInt("seenInBiddingLookBack", 1)

    // detect recent seed raw data path in airflow and pass to spark job
    val seedRawDataRecentVersion = config.getString("seedRawDataRecentVersion", "None")
    val seedRawDataS3Bucket = S3Utils.refinePath(config.getString("seedRawDataS3Bucket", "ttd-datprd-us-east-1"))
    val seedRawDataS3Path = S3Utils.refinePath(config.getString("seedRawDataS3Path", "prod/data/Seed/v=1/SeedId="))

    // n bid impressions we care about
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

    val bidImpressionRepartitionNumAfterFilter = config.getInt("bidImpressionRepartitionNumAfterFilter", 8192)

    val seedCoalesceAfterFilter = config.getInt("seedCoalesceAfterFilter", 3)
    
    // the way to determine the n tdid selection->
    // 0: last n tdid; 1: even stepwise selection for n tdid; 2 and other: random select n tdid
    val tdidTouchSelection = config.getInt("tdidTouchSelection", default = 0)
  }

object AudienceModelInputGeneratorJob {
  object SubFolder extends Enumeration {
    type SubFolder = Value
    val Val, Holdout, Train = Value
  }

  def main(args: Array[String]): Unit = {
    runETLPipeline()
  }

  def runETLPipeline(): Unit = {
    val policyTable = clusterTargetingData(audienceModelInputConfig.model, audienceModelInputConfig.supportedDataSources, 
    audienceModelInputConfig.seedSizeLowerScaleThreshold, audienceModelInputConfig.seedSizeUpperScaleThreshold)
    val date = dateTime.toLocalDate

    policyTable.foreach(typePolicyTable => {
      val dataset = {
        audienceModelInputConfig.model match {
          case Model.AEM =>
            typePolicyTable match {
              case ((DataSource.SIB, CrossDeviceVendor.None), subPolicyTable: Array[AudienceModelPolicyRecord]) =>
                AEMSIBInputGenerator.generateDataset(date, subPolicyTable, 
                seenInBiddingLookBack=audienceModelInputConfig.seenInBiddingLookBack, 
                minimalPositiveLabelSizeOfSIB=audienceModelInputConfig.minimalPositiveLabelSizeOfSIB)
              case ((DataSource.Conversion, CrossDeviceVendor.None), subPolicyTable: Array[AudienceModelPolicyRecord]) =>
                AEMConversionInputGenerator.generateDataset(date, subPolicyTable,
                conversionLookBack=audienceModelInputConfig.conversionLookBack,
                bidImpressionRepartitionNumAfterFilter=audienceModelInputConfig.bidImpressionRepartitionNumAfterFilter)
              case _ => throw new Exception(s"unsupported policy settings: Model[${Model.AEM}], Setting[${typePolicyTable._1}]")
            }
          case Model.RSM =>
            typePolicyTable match {
              case ((DataSource.Seed, CrossDeviceVendor.None), subPolicyTable: Array[AudienceModelPolicyRecord]) =>
                RSMSeedInputGenerator.generateDataset(date, subPolicyTable,
                seedRawDataS3Path=audienceModelInputConfig.seedRawDataS3Path,
                seedRawDataS3Bucket=audienceModelInputConfig.seedRawDataS3Bucket,
                seedRawDataRecentVersion=audienceModelInputConfig.seedRawDataRecentVersion,
                seedCoalesceAfterFilter=audienceModelInputConfig.seedCoalesceAfterFilter,
                bidImpressionRepartitionNumAfterFilter=audienceModelInputConfig.bidImpressionRepartitionNumAfterFilter)
              case _ => throw new Exception(s"unsupported policy settings: Model[${Model.RSM}], Setting[${typePolicyTable._1}]")
            }
          case _ => throw new Exception(s"unsupported Model[${audienceModelInputConfig.model}]")
        }
      }

      val resultSet = ModelFeatureTransform.modelFeatureTransform[AudienceModelInputRecord](dataset)
        .withColumn("SplitRemainder", hash(concat('TDID, lit(audienceModelInputConfig.saltToSplitDataset))) % audienceModelInputConfig.validateDatasetSplitModule)
        .withColumn("SubFolder",
          when('SplitRemainder === lit(SubFolder.Val.id), SubFolder.Val.id)
            .when('SplitRemainder === lit(SubFolder.Holdout.id), SubFolder.Holdout.id)
            .otherwise(SubFolder.Train.id))

      resultSet.cache()

      AudienceModelInputDataset(audienceModelInputConfig.model.toString, s"${typePolicyTable._1._1}_${typePolicyTable._1._2}").writePartition(
        resultSet.filter('SubFolder === lit(SubFolder.Val.id)).as[AudienceModelInputRecord],
        dateTime,
        subFolderKey = Some(audienceModelInputConfig.subFolder),
        subFolderValue = Some(SubFolder.Val.toString),
        format = Some("tfrecord"),
        saveMode = SaveMode.Overwrite
      )

      if (audienceModelInputConfig.persistHoldoutSet) {
        AudienceModelInputDataset(audienceModelInputConfig.model.toString, s"${typePolicyTable._1._1}_${typePolicyTable._1._2}").writePartition(
          resultSet.filter('SubFolder === lit(SubFolder.Holdout.id)).as[AudienceModelInputRecord],
          dateTime,
          subFolderKey = Some(audienceModelInputConfig.subFolder),
          subFolderValue = Some(SubFolder.Holdout.toString),
          format = Some("tfrecord"),
          saveMode = SaveMode.Overwrite
        )
      }

      AudienceModelInputDataset(audienceModelInputConfig.model.toString, s"${typePolicyTable._1._1}_${typePolicyTable._1._2}").writePartition(
        resultSet.filter('SubFolder === lit(SubFolder.Train.id)).as[AudienceModelInputRecord],
        dateTime,
        subFolderKey = Some(audienceModelInputConfig.subFolder),
        subFolderValue = Some(SubFolder.Train.toString),
        format = Some("tfrecord"),
        saveMode = SaveMode.Overwrite
      )

      resultSet.unpersist()
    }
    )
  }

  def clusterTargetingData(model: Model.Value, supportedDataSources: Array[Int], seedSizeLowerScaleThreshold: Int, seedSizeUpperScaleThreshold: Int): 
  Map[(DataSource.DataSource, CrossDeviceVendor.CrossDeviceVendor), Array[AudienceModelPolicyRecord]] = {
    val policyTable = AudienceModelPolicyReadableDataset(model)
      .readSinglePartition(dateTime)(spark)
      .where((length('ActiveSize)>=seedSizeLowerScaleThreshold) && (length('ActiveSize)<seedSizeUpperScaleThreshold))
      // .where('SourceId==="1ufp35u0")
      .where('IsActive)
      .where('Source.isin(supportedDataSources: _*))
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
  // set the default sampling ratio as 10%
  val samplingFunction = shouldConsiderTDID3(config.getInt(s"userDownSampleHitPopulation${name}", default = 100000), config.getString(s"saltToSampleUser${name}", default = "0BgGCE"))(_)
  val prometheus = new PrometheusClient("AudienceModel", name)
  val mappingFunctionGenerator =
    (dictionary: Map[Any, Int]) =>
      udf((values: Array[Any]) =>
        values.filter(dictionary.contains).map(dictionary.getOrElse(_, -1)))

  val stringEqUdf = udf((l: String, r: String) => l == r)

  /**
   * Core logic to generate model training dataset should be put here
   */
  def generateDataset(date: LocalDate, policyTable: Array[AudienceModelPolicyRecord], seenInBiddingLookBack: Int = 1, 
  minimalPositiveLabelSizeOfSIB: Int = 0, conversionLookBack: Int = 1, 
  bidImpressionRepartitionNumAfterFilter: Int = 8192, seedRawDataS3Path: String = "None", 
  seedRawDataS3Bucket: String = "None", seedRawDataRecentVersion: String = "None", seedCoalesceAfterFilter: Int = 3): 
  DataFrame = {
    val (sampledBidsImpressionsKeys, bidsImpressionsLong, uniqueTDIDs) = getBidImpressions(date, audienceModelInputConfig.lastTouchNumberInBR,
      audienceModelInputConfig.tdidTouchSelection)


    val rawLabels = generateLabels(date, policyTable, seenInBiddingLookBack, minimalPositiveLabelSizeOfSIB, conversionLookBack, 
    bidImpressionRepartitionNumAfterFilter, seedRawDataS3Path, seedRawDataS3Bucket, seedRawDataRecentVersion, seedCoalesceAfterFilter)

    val filteredLabels = rawLabels.join(uniqueTDIDs, Seq("TDID"), "inner")


    val refinedLabels = sampleLabels(filteredLabels, policyTable)

    val roughResult = bidsImpressionsLong
      .drop("CampaignId", "LogEntryTime")
      .join(
        refinedLabels
          .join(
            sampledBidsImpressionsKeys, Seq("TDID"), "inner"),
        Seq("TDID", "BidRequestId"), "inner")
//      .where(stringEqUdf('BidRequestId, 'BidRequestId2))

    if (audienceModelInputConfig.recordIntermediateResult) {
      filteredLabels.write.mode("overwrite").parquet(s"s3a://thetradedesk-mlplatform-us-east-1/data/${ttdEnv}/audience/uniEtlTestIntermediate/${name}/filteredLabels/date=${date.format(DateTimeFormatter.BASIC_ISO_DATE)}")
      rawLabels.write.mode("overwrite").parquet(s"s3a://thetradedesk-mlplatform-us-east-1/data/${ttdEnv}/audience/uniEtlTestIntermediate/${name}/rawLabels/date=${date.format(DateTimeFormatter.BASIC_ISO_DATE)}")
      refinedLabels.write.mode("overwrite").parquet(s"s3a://thetradedesk-mlplatform-us-east-1/data/${ttdEnv}/audience/uniEtlTestIntermediate/${name}/refinedLabels/date=${date.format(DateTimeFormatter.BASIC_ISO_DATE)}")
      roughResult.write.mode("overwrite").parquet(s"s3a://thetradedesk-mlplatform-us-east-1/data/${ttdEnv}/audience/uniEtlTestIntermediate/${name}/roughResult/date=${date.format(DateTimeFormatter.BASIC_ISO_DATE)}")
    
    }

    refineResult(roughResult)
  }

  def refineResult(roughResult: DataFrame): DataFrame = {
    /* TODO add weighted downSample logic to refine positive label size and negative label size
     *   https://atlassian.thetradedesk.com/confluence/display/EN/ETL+and+model+training+pipline+based+on+SIB+dataset
     */
    roughResult
  }

  def generateLabels(date: LocalDate, policyTable: Array[AudienceModelPolicyRecord], seenInBiddingLookBack: Int = 1, 
  minimalPositiveLabelSizeOfSIB: Int = 0, conversionLookBack: Int = 1, 
  bidImpressionRepartitionNumAfterFilter: Int = 8192, seedRawDataS3Path: String = "None", 
  seedRawDataS3Bucket: String = "None", seedRawDataRecentVersion: String = "None", seedCoalesceAfterFilter: Int = 3): DataFrame

  def getBidImpressions(date: LocalDate, Ntouch: Int, tdidTouchSelection: Int = 0) = {
    val bidImpressionsS3Path = BidsImpressions.BIDSIMPRESSIONSS3 + "prod/bidsimpressions/"

    val bidsImpressionsLong = loadParquetData[BidsImpressionsSchema](bidImpressionsS3Path, date, lookBack=Some(audienceModelInputConfig.bidImpressionLookBack), source = Some(GERONIMO_DATA_SOURCE))
      .withColumnRenamed("UIID", "TDID")
      .filter(samplingFunction('TDID))
      .select('BidRequestId, // use to connect with bidrequest, to get more features
        'AdvertiserId,
        'AdGroupId,
        'SupplyVendor,
        'DealId,
        'SupplyVendorPublisherId,
        'AliasedSupplyPublisherId,
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
      .repartition(audienceModelInputConfig.bidImpressionRepartitionNumAfterFilter, 'TDID)
      .cache()

    val sampledBidsImpressionsKeys = ApplyNTouchOnSameTdid(
      bidsImpressionsLong.select('BidRequestId, 'TDID, 'CampaignId, 'LogEntryTime), 
      Ntouch, tdidTouchSelection)
      .cache()

    val uniqueTDIDs = sampledBidsImpressionsKeys.select('TDID).distinct().cache()

    (sampledBidsImpressionsKeys, bidsImpressionsLong, uniqueTDIDs)
  }

  def ApplyNTouchOnSameTdid(sampledBidsImpressionsKeys: DataFrame, Ntouch: Int, tdidTouchSelection: Int = 0): DataFrame = {
    val lastWindow = Window.partitionBy('TDID).orderBy('LogEntryTime.desc)
    val stepWindow = Window.partitionBy('TDID)
    val randomWindow = Window.partitionBy('TDID).orderBy('randomRow.desc)
    var touchSample: DataFrame = null
    if (tdidTouchSelection==0) {
      touchSample = sampledBidsImpressionsKeys
      .withColumn("row", row_number().over(lastWindow))
      .filter('row <= Ntouch)
      .drop('LogEntryTime)
      .drop('row)
    } else if (tdidTouchSelection==1) {
      touchSample = sampledBidsImpressionsKeys
      .withColumn("row", row_number().over(stepWindow))
      .withColumn("TDIDCount", count("TDID").over(stepWindow))
      .filter('row % ceil('TDIDCount/Ntouch.toDouble) === lit(1.0))
      .drop('LogEntryTime)
      .drop('row)
      .drop('TDIDCount)
    } else {
      touchSample = sampledBidsImpressionsKeys
      .withColumn("randomRow", rand())
      .withColumn("row", row_number().over(randomWindow))
      .filter('row <= Ntouch)
      .drop('LogEntryTime)
      .drop('row)
      .drop('randomRow)
    }
    touchSample
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

    val labelDatasetSize = labels.count()
    // the default sampling ratio is 10%
    val downSampleFactor = config.getInt(s"userDownSampleHitPopulation${name}", default = 100000)/1000000.0

    val syntheticIdToPolicy = policyTable
      .map(e => (e.SyntheticId, e))
      .toMap

    val positiveSampleUDF = positiveSampleUDFGenerator(
      syntheticIdToPolicy,
      audienceModelInputConfig.positiveSampleUpperThreshold,
      audienceModelInputConfig.positiveSampleLowerThreshold,
      audienceModelInputConfig.positiveSampleSmoothingFactor,
      downSampleFactor
      )

    // val labelDatasetSize = (labels.count()*(1000000.0/config.getInt(s"userDownSampleHitPopulation${name}", default = 1000000))).toLong
 
    val aboveThresholdPolicyTable = policyTable
      .filter(e => e.ActiveSize >= audienceModelInputConfig.positiveSampleLowerThreshold)

    val negativeSampleUDF = negativeSampleUDFGenerator(
      aboveThresholdPolicyTable,
      audienceModelInputConfig.positiveSampleUpperThreshold,
      labelDatasetSize,
      downSampleFactor
      )

    // downsample positive labels to keep # of positive labels among targets balanced
    val labelResult = labels
      .withColumn("PositiveSamples", positiveSampleUDF('PositiveSyntheticIds))
      .withColumn("NegativeSamples", negativeSampleUDF(lit(audienceModelInputConfig.negativeSampleRatio) * size(col("PositiveSamples"))))
      .withColumn("NegativeSamples", array_except(col("NegativeSamples"), 'PositiveSyntheticIds))
      .withColumn("PositiveTargets", getLabels(1f)(size($"PositiveSamples")))
      .withColumn("NegativeTargets", getLabels(0f)(size($"NegativeSamples")))
      .withColumn("SyntheticIds", concat($"PositiveSamples", $"NegativeSamples"))
      .withColumn("Targets", concat($"PositiveTargets", $"NegativeTargets"))
      .select('TDID, 'SyntheticIds, 'Targets)
      // partialy explode the result to keep the target array within the max length
      .withColumn("ZippedTargets", zipAndGroupUDFGenerator(audienceModelInputConfig.labelMaxLength)('SyntheticIds, 'Targets))
      .select(col("TDID"), explode(col("ZippedTargets")).as("ZippedTargets"))
      .select(col("TDID"), col("ZippedTargets").getField("_1").as("SyntheticIds"), col("ZippedTargets").getField("_2").as("Targets"))

    labelResult
  }
}

/**
 * This class is used to generate model training samples for first party pixel model
 * using seenInBidding dataset
 
 */
object AEMSIBInputGenerator extends AudienceModelInputGenerator("AEMSIB") {

  override def generateLabels(date: LocalDate, policyTable: Array[AudienceModelPolicyRecord], seenInBiddingLookBack: Int = 1, 
  minimalPositiveLabelSizeOfSIB: Int = 0, conversionLookBack: Int = 1, 
  bidImpressionRepartitionNumAfterFilter: Int = 8192, seedRawDataS3Path: String = "None", 
  seedRawDataS3Bucket: String = "None", seedRawDataRecentVersion: String = "None", seedCoalesceAfterFilter: Int = 3): 
  DataFrame = {
    val mappingFunction = mappingFunctionGenerator(
      policyTable.map(e => (e.TargetingDataId, e.SyntheticId)).toMap)

    // FIXME use cross device SIB dataset to replace this one
    SeenInBiddingV3DeviceDataSet().readPartition(date, lookBack = Some(seenInBiddingLookBack))(spark)
      .withColumnRenamed("DeviceId", "TDID")
      .filter(samplingFunction('TDID))
      // only support first party targeting data ids in current solution
      .withColumn("PositiveSyntheticIds", mappingFunction('FirstPartyTargetingDataIds))
      .filter(size('PositiveSyntheticIds) > minimalPositiveLabelSizeOfSIB)
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


  override def generateLabels(date: LocalDate, policyTable: Array[AudienceModelPolicyRecord], seenInBiddingLookBack: Int = 1, 
  minimalPositiveLabelSizeOfSIB: Int = 0, conversionLookBack: Int = 1, 
  bidImpressionRepartitionNumAfterFilter: Int = 8192, seedRawDataS3Path: String = "None", 
  seedRawDataS3Bucket: String = "None", seedRawDataRecentVersion: String = "None", seedCoalesceAfterFilter: Int = 3): 
  DataFrame = {
    val mappingRows = policyTable.
      map(e => Row(e.SourceId, e.SyntheticId))
      .toSeq

    val mappingDataset = broadcast(
      spark.createDataFrame(
        spark.sparkContext.parallelize(mappingRows),
        mappingSchema))

    ConversionDataset(defaultCloudProvider)
      .readRange(date.minusDays(conversionLookBack).atStartOfDay(), date.plusDays(1).atStartOfDay())
      .select('TDID, 'TrackingTagId)
      .filter(samplingFunction('TDID))
      .repartition(bidImpressionRepartitionNumAfterFilter, 'TDID)
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
  override def generateLabels(date: LocalDate, policyTable: Array[AudienceModelPolicyRecord], seenInBiddingLookBack: Int = 1, 
  minimalPositiveLabelSizeOfSIB: Int = 0, conversionLookBack: Int = 1, 
  bidImpressionRepartitionNumAfterFilter: Int = 8192, seedRawDataS3Path: String = "None", 
  seedRawDataS3Bucket: String = "None", seedRawDataRecentVersion: String = "None", seedCoalesceAfterFilter: Int = 3): 
  DataFrame = {
    policyTable.par.map(
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
        val seedDataPath = seedRawDataS3Path + record.SourceId
        val seedDataFullPath = "s3a://" + seedRawDataS3Bucket + "/" + seedDataPath
        // todo solve the problem when the current seed is not updated in s3
        val recentVersion =
          if (seedRawDataRecentVersion != "None") seedRawDataRecentVersion
          else S3Utils.queryCurrentDataVersion(seedRawDataS3Bucket, seedDataPath)

        try {
          Some(spark.read.parquet(seedDataFullPath + "/" + recentVersion)
          .select('UserId)
          .withColumnRenamed("UserId", "TDID")
          .filter(samplingFunction('TDID))
          .coalesce(seedCoalesceAfterFilter)
          .withColumn("SyntheticId", lit(record.SyntheticId))
          .select('TDID, 'SyntheticId))
        } catch {
          case e: Exception => 
          println(s"Caught exception of type ${e.getClass} on seed ${record.SourceId}")
          None
        }
        
      }
    ).toArray.flatten.reduce(_ unionAll _)
      .repartition(bidImpressionRepartitionNumAfterFilter, 'TDID)
      .groupBy('TDID)
      .agg(collect_list('SyntheticId) as "PositiveSyntheticIds")
      .cache()
  }
}