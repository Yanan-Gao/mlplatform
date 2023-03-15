package com.thetradedesk.audience.jobs

import com.thetradedesk.audience.datasets.{ConversionDataset, CrossDeviceGraphDataset, FirstPartyPixelModelInputDataset, FirstPartyPixelModelInputRecord, SampledCrossDeviceGraphDataset, SeenInBiddingV3DeviceDataSet, TargetingDataDataset, TrackingTagDataset, UniversalPixelDataset, UniversalPixelTrackingTagDataset}
import com.thetradedesk.audience.{date, sampleHit, shouldConsiderTDID2, shouldConsiderTDID, trainSetDownSampleFactor, userDownSampleBasePopulation}
import com.thetradedesk.audience.sample.DownSample.hashSampleV2
import com.thetradedesk.audience.transform.{FirstPartyDataTransform, ModelFeatureTransform}
import com.thetradedesk.geronimo.bidsimpression.schema.{BidsImpressions, BidsImpressionsSchema}
import com.thetradedesk.geronimo.shared.{GERONIMO_DATA_SOURCE, loadParquetData}
import com.thetradedesk.spark.sql.SQLFunctions._
import com.thetradedesk.spark.TTDSparkContext.spark
import com.thetradedesk.spark.TTDSparkContext.spark.implicits._
import com.thetradedesk.spark.util.io.FSUtils
import com.thetradedesk.spark.util.prometheus.PrometheusClient
import com.thetradedesk.spark.util.TTDConfig.{config, defaultCloudProvider}

import java.time.LocalDate
import java.time.format.DateTimeFormatter
import org.apache.spark.sql.{Column, DataFrame, Dataset, SaveMode}
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._

import java.time.LocalDate

abstract class FirstPartyPixelDailyModelInputGeneratorV2 {

  val samplingFunction = shouldConsiderTDID2 _

  object Config {
    val numTDID = config.getInt("numTDID", 100)

    val labelLookBack = config.getInt("labelLookBack", 0)

    val selectedPixelsConfigPath = config.getString("selectedPixelsConfigPath", "s3a://thetradedesk-useast-hadoop/Data_Science/freeman/audience_extension/firstPixel46_TargetingDataId/")

    val seedToSplitDataset = config.getString("seedToSplitDataset", "@5u*&5vh")

    val validateDatasetSplitModule = config.getInt("validateDatasetSplitModule", default = 5)
  }

  def main(args: Array[String]): Unit = {
    val prometheus = new PrometheusClient("FirstPartyPixelModel", this.getClass.getSimpleName)

    val allTargetingDataIds = groupTargetData()

    // TODO Group detected targeting data ids and process
    allTargetingDataIds.foreach(
      runETLPipeline
    )
  }

  def runETLPipeline(allTargetingDataIds: Array[Long]): Unit = {
    val dataset = generateDataset(date, spark.sparkContext.broadcast(allTargetingDataIds))
    dataset.withColumn("isValidate", hash(concat('TDID, lit(Config.seedToSplitDataset))) % Config.validateDatasetSplitModule === lit(0))


  }

  def groupTargetData(): Array[Array[Long]] = {
    Array(spark.read.parquet(Config.selectedPixelsConfigPath).as[Long].collect)
  }

  def generateDataset(date: LocalDate, allTargetingDataIds: org.apache.spark.broadcast.Broadcast[_]): DataFrame

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
        'Longitude,
        'MatchedFoldPosition,
        'InternetConnectionType,
        'OperatingSystem,
        'sin_minute_hour,
        'cos_minute_hour,
        'sin_minute_day,
        'cos_minute_day
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

    val sampledBidsImpressionsKeys = loadParquetData[BidsImpressionsSchema](bidImpressionsS3Path, date.minusDays(Config.labelLookBack), source = Some(GERONIMO_DATA_SOURCE))
      .withColumnRenamed("UIID", "TDID")
      .select('BidRequestId, 'TDID, 'CampaignId)
      .filter(samplingFunction('TDID)) // in the future, we may not have the id, good to think about how to solve

    (sampledBidsImpressionsKeys, bidsImpressionsLong)
  }
}

object FirstPartyPixelDailySIBModelInputGeneratorV2 extends FirstPartyPixelDailyModelInputGeneratorV2 {

  def generateDataset(date: LocalDate, allTargetingDataIds: org.apache.spark.broadcast.Broadcast[_]): DataFrame = {
    val (sampledBidsImpressionsKeys, bidsImpressionsLong) = getBidImpressions(date)
    val labels = readSeenInBiddingData(date, allTargetingDataIds)

    // TODO dedup sampledBidsImpressionsKeys in case huge impressions for same TDID
    val roughDataset = sampledBidsImpressionsKeys
      .join(refineLabels(labels), Seq("TDID"), "inner")
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

  def readSeenInBiddingData(date: LocalDate, allTargetingDataIds: org.apache.spark.broadcast.Broadcast[_]): DataFrame = {
    // FIXME use cross device SIB dataset to replace this one
    SeenInBiddingV3DeviceDataSet().readPartition(date, lookBack = Some(Config.labelLookBack))(spark)
      .withColumnRenamed("DeviceId", "TDID")
      .filter(samplingFunction('TDID))
      // only support first party targeting data ids in current solution
      .withColumn("PositiveTargetingDataIds", array_intersect('FirstPartyTargetingDataIds, typedLit(allTargetingDataIds.value)))
      .withColumn("NegativeTargetingDataIds", array_except(typedLit(allTargetingDataIds.value), 'PositiveTargetingDataIds))
      .select('TDID, 'PositiveTargetingDataIds, 'NegativeTargetingDataIds)
  }
}