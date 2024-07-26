package com.thetradedesk.audience.jobs.modelinput

import com.thetradedesk.audience._
import com.thetradedesk.audience.configs.AudienceModelInputGeneratorConfig
import com.thetradedesk.audience.datasets.CrossDeviceVendor.CrossDeviceVendor
import com.thetradedesk.audience.datasets._
import com.thetradedesk.audience.sample.WeightSampling.{getLabels, negativeSampleUDFGenerator, positiveSampleUDFGenerator, zipAndGroupUDFGenerator}
import com.thetradedesk.audience.transform.ContextualTransform.generateContextualFeatureTier1
import com.thetradedesk.audience.transform.ExtendArrayTransforms.seedIdToSyntheticIdMapping
import com.thetradedesk.audience.utils.DateUtils
import com.thetradedesk.geronimo.bidsimpression.schema.{BidsImpressions, BidsImpressionsSchema}
import com.thetradedesk.geronimo.shared.transform.ModelFeatureTransform
import com.thetradedesk.geronimo.shared.{GERONIMO_DATA_SOURCE, loadParquetData, readModelFeatures}
import com.thetradedesk.spark.TTDSparkContext.spark
import com.thetradedesk.spark.TTDSparkContext.spark.implicits._
import com.thetradedesk.spark.util.TTDConfig.{config, defaultCloudProvider}
import com.thetradedesk.spark.util.io.FSUtils
import com.thetradedesk.spark.util.prometheus.PrometheusClient
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.{IntegerType, StringType, StructField, StructType, DoubleType}
import org.apache.spark.sql.{Column, DataFrame, Row, SaveMode}

import java.time.LocalDate
import java.time.format.DateTimeFormatter
import scala.util.Try

/**
 * This is the base class for audience model training data generation
 * including AEM(audience extension model), RSM(relevance score model), etc
 */
abstract class AudienceModelInputGenerator(name: String, val sampleRate: Double) {
  // set the default sampling ratio as 10%
  // val sampleRate = 1.0
  val samplingFunction = shouldConsiderTDID3((config.getInt(s"userDownSampleHitPopulation${name}", default = 100000)*sampleRate).toInt, config.getString(s"saltToSampleUser${name}", default = "0BgGCE"))(_)

  val mappingFunctionGenerator =
    (dictionary: Map[Any, Int]) =>
      udf((values: Array[Any]) =>
        values.filter(dictionary.contains).map(dictionary.getOrElse(_, -1)))

  /**
   * Core logic to generate model training dataset should be put here
   */
  def generateDataset(date: LocalDate, policyTable: Array[AudienceModelPolicyRecord]):
  DataFrame = {
    val (sampledBidsImpressionsKeys, bidsImpressionsLong, uniqueTDIDs) = getBidImpressions(date, AudienceModelInputGeneratorConfig.lastTouchNumberInBR,
      AudienceModelInputGeneratorConfig.tdidTouchSelection)


    val rawLabels = generateLabels(date, policyTable)

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

    if (AudienceModelInputGeneratorConfig.recordIntermediateResult) {
      filteredLabels.write.mode("overwrite").parquet(s"s3a://thetradedesk-mlplatform-us-east-1/data/${ttdEnv}/audience/uniEtlTestIntermediate/${name}/filteredLabels/date=${date.format(DateTimeFormatter.BASIC_ISO_DATE)}")
      rawLabels.write.mode("overwrite").parquet(s"s3a://thetradedesk-mlplatform-us-east-1/data/${ttdEnv}/audience/uniEtlTestIntermediate/${name}/rawLabels/date=${date.format(DateTimeFormatter.BASIC_ISO_DATE)}")
      refinedLabels.write.mode("overwrite").parquet(s"s3a://thetradedesk-mlplatform-us-east-1/data/${ttdEnv}/audience/uniEtlTestIntermediate/${name}/refinedLabels/date=${date.format(DateTimeFormatter.BASIC_ISO_DATE)}")
      roughResult.write.mode("overwrite").parquet(s"s3a://thetradedesk-mlplatform-us-east-1/data/${ttdEnv}/audience/uniEtlTestIntermediate/${name}/roughResult/date=${date.format(DateTimeFormatter.BASIC_ISO_DATE)}")

    }

    extendFeatures(refineResult(roughResult))
  }

  def refineResult(roughResult: DataFrame): DataFrame = {
    /* TODO add weighted downSample logic to refine positive label size and negative label size
     *   https://atlassian.thetradedesk.com/confluence/display/EN/ETL+and+model+training+pipline+based+on+SIB+dataset
     */
    roughResult
  }

  def extendFeatures(refinedResult: DataFrame): DataFrame = {
    generateContextualFeatureTier1(refinedResult)
  }

  def generateLabels(date: LocalDate, policyTable: Array[AudienceModelPolicyRecord]): DataFrame

  def getBidImpressions(date: LocalDate, Ntouch: Int, tdidTouchSelection: Int = 0) = {
    val bidImpressionsS3Path = BidsImpressions.BIDSIMPRESSIONSS3 + "prod/bidsimpressions/"

    val bidsImpressionsLong = loadParquetData[BidsImpressionsSchema](bidImpressionsS3Path, date, lookBack=Some(AudienceModelInputGeneratorConfig.bidImpressionLookBack), source = Some(GERONIMO_DATA_SOURCE))
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
        'LogEntryTime,
        'ContextualCategories,
        'MatchedSegments,
        'UserSegmentCount,
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
      .withColumn("MatchedSegmentsLength", when('MatchedSegments.isNull,0.0).otherwise(size('MatchedSegments).cast(DoubleType)))
      .withColumn("HasMatchedSegments", when('MatchedSegments.isNull,0).otherwise(1))
      .withColumn("UserSegmentCount", when('UserSegmentCount.isNull, 0.0).otherwise('UserSegmentCount.cast(DoubleType)))
      .repartition(AudienceModelInputGeneratorConfig.bidImpressionRepartitionNumAfterFilter, 'TDID)
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
      .withColumn("row", row_number().over(lastWindow))
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
    // for incremental training, we intentionally keep the down sample factor the same as the full train data then we will have proportional extra sample rate of the full train data
    val downSampleFactor = config.getInt(s"userDownSampleHitPopulation${name}", default = 100000)  * 1.0 / userDownSampleBasePopulation

    val syntheticIdToPolicy = policyTable
      .map(e => (e.SyntheticId, e))
      .toMap

    val positiveSampleUDF = positiveSampleUDFGenerator(
      syntheticIdToPolicy,
      AudienceModelInputGeneratorConfig.positiveSampleUpperThreshold,
      AudienceModelInputGeneratorConfig.positiveSampleLowerThreshold,
      AudienceModelInputGeneratorConfig.positiveSampleSmoothingFactor,
      downSampleFactor
      )

    // val labelDatasetSize = (labels.count()*(1000000.0/config.getInt(s"userDownSampleHitPopulation${name}", default = 1000000))).toLong

    val aboveThresholdPolicyTable = policyTable
      .filter(e => e.ActiveSize * downSampleFactor >= AudienceModelInputGeneratorConfig.positiveSampleLowerThreshold)

    val negativeSampleUDF = negativeSampleUDFGenerator(
      aboveThresholdPolicyTable,
      AudienceModelInputGeneratorConfig.positiveSampleUpperThreshold,
      labelDatasetSize,
      downSampleFactor
      )

    // downsample positive labels to keep # of positive labels among targets balanced
    val labelResult = labels
      .withColumn("PositiveSamples", positiveSampleUDF('PositiveSyntheticIds))
      .withColumn("NegativeSamples", negativeSampleUDF(lit(AudienceModelInputGeneratorConfig.negativeSampleRatio) * size(col("PositiveSamples"))))
      .withColumn("NegativeSamples", array_except(col("NegativeSamples"), 'PositiveSyntheticIds))
      .withColumn("PositiveTargets", getLabels(1f)(size($"PositiveSamples")))
      .withColumn("NegativeTargets", getLabels(0f)(size($"NegativeSamples")))
      .withColumn("SyntheticIds", concat($"PositiveSamples", $"NegativeSamples"))
      .withColumn("Targets", concat($"PositiveTargets", $"NegativeTargets"))
      .select('TDID, 'GroupId, 'SyntheticIds, 'Targets)
      // partialy explode the result to keep the target array within the max length
      .withColumn("ZippedTargets", zipAndGroupUDFGenerator(AudienceModelInputGeneratorConfig.labelMaxLength)('SyntheticIds, 'Targets))
      .select(col("TDID"), 'GroupId, explode(col("ZippedTargets")).as("ZippedTargets"))
      .select(col("TDID"), 'GroupId, col("ZippedTargets").getField("_1").as("SyntheticIds"), col("ZippedTargets").getField("_2").as("Targets"))

    labelResult
  }
}