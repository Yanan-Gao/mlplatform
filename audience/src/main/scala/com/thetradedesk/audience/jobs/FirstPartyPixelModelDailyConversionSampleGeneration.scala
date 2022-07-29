package com.thetradedesk.audience.jobs

import com.thetradedesk.audience.datasets.{BidsImpressionDataSet, ConversionDataset, ConversionRecord, FirstPartyPixelModelInputDataset, FirstPartyPixelModelInputRecord, TrackingTagDataset}
import com.thetradedesk.audience.date
import com.thetradedesk.spark.TTDSparkContext.spark
import com.thetradedesk.spark.util.TTDConfig.config
import com.thetradedesk.audience.sample.DownSample.hashSampleV2
import com.thetradedesk.audience.transform.ModelFeatureTransform
import com.thetradedesk.spark.TTDSparkContext.spark.implicits._
import com.thetradedesk.spark.sql.SQLFunctions._
import com.thetradedesk.spark.util.prometheus.PrometheusClient
import org.apache.spark.sql.{DataFrame, Dataset}
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._

object FirstPartyPixelModelDailyConversionSampleGeneration {
  private val conversionLookBack = config.getInt("conversionLookBack", 1)
  private val bidsImpressionLookBack = config.getInt("bidsImpressionLookBack", 3)
  private val sampleMod = config.getInt("sampleMod", 10)
  private val sampleHit = config.getString("sampleHit", "0")
  private val sampleSeed = config.getString("sampleSeed", "FirstPartyPixel")
  private val subFolder = config.getString("subFolder", "training")
  private val numTDID = config.getInt("numTDID", 100)
  // control maximum positive records for a pxiel on given day; without it, the data size will be huge
  private val maxPos = config.getInt("numTDID", default = 6000)
  // used for the hard negative samples generation
  private val negFactor = config.getInt("negFactor", default = 10)
  private val selectedPixelsConfigPath = config.getString("selectedPixelsConfigPath", "s3a://thetradedesk-useast-hadoop/Data_Science/Yang/audience_extension/selected_1pp/firstPixel200/")


  def main(args: Array[String]): Unit = {
    val prometheus = new PrometheusClient("FirstPartyPixelModel", "DailyConversionSampleGeneration")
    val positiveSampledCount = prometheus.createGauge("positive_sample_count", "Number of positive sampled records")
    val softNegativeSampledCount = prometheus.createGauge("soft_negative_sample_count", "Number of soft negative sampled records")
    val hardNegativeSampledCount = prometheus.createGauge("hard_negative_sample_count", "Number of hard negative sampled records")

    val conversionDS = ConversionDataset().readPartition(date, lookBack = Some(conversionLookBack))
      .select('TDID, 'TrackingTagId)
      .filter('TDID.isNotNullOrEmpty && 'TDID =!= "00000000-0000-0000-0000-000000000000")
    val bidsImpressions = BidsImpressionDataSet().readPartition(date, lookBack = Some(bidsImpressionLookBack))
      .withColumnRenamed("UIID", "TDID")
      .filter('TDID.isNotNullOrEmpty && 'TDID =!= "00000000-0000-0000-0000-000000000000")  // in the future, we may not have the id, good to think about how to solve
      .select('BidRequestId, // use to connect with bidrequest, to get more features
        'LogEntryTime,
        'AdvertiserId,
        'TDID,
        'CampaignId,
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
        // $"a.AdGroupIdInteger", // on bidrequest
        'DeviceType,
        'OperatingSystemFamily,
        'Browser,
        // $"a.InternetConnectionType", // on bidrequest
        // 'MatchedFoldPosition,
        'sin_hour_week,  // time based features sometime are useful than expected
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
      .withColumn("Date", to_date('LogEntryTime, "yyyy-MM-dd")) // used for saving the data into different folds or train and val
      .withColumn("AdWidthInPixels", ('AdWidthInPixels - lit(1.0))/lit(9999.0)) // 1 - 10000
      .withColumn("AdHeightInPixels", ('AdHeightInPixels - lit(1.0))/lit(9999.0)) // 1 - 10000
      .withColumn("Latitude", ('Latitude + lit(90.0))/lit(180.0))  // -90 - 90
      .withColumn("Longitude", ('Longitude + lit(180.0))/lit(360.0)) //-180 - 180

    val trackingTagDataset = TrackingTagDataset().readPartition(date)

    val sampledConversionDS = hashSampleV2(conversionDS, "TDID", sampleMod, sampleSeed, sampleHit).cache()
    val sampledBidsImpressions = hashSampleV2(bidsImpressions, "TDID", sampleMod, sampleSeed, sampleHit).cache()

    val selectedConv = selectedPixelsConfigPath match {
      case x if x.isEmpty => sampledConversionDS
        .select('TrackingTagId, 'TDID)
        .join(broadcast(trackingTagDataset),
          Seq("TrackingTagId"),
          "inner")
        .select('TargetingDataId, 'TDID)
        .distinct()
      case _ =>
        val firstPixel200 = spark.read.parquet(selectedPixelsConfigPath)
          .join(broadcast(trackingTagDataset),
            Seq("TrackingTagId"),
            "inner")

        // conversion dataset with selected pixels
        sampledConversionDS
          .select('TrackingTagId, 'TDID)
          .join(broadcast(firstPixel200),
            Seq("TrackingTagId"),
            "inner")
          .select('TargetingDataId, 'TDID)
          .distinct()
    }

    val candidatePool = sampledBidsImpressions
      .join(broadcast(selectedConv), Seq("TDID"), "left")
      .cache()

    // generate positive samples
    val window = Window.partitionBy('TargetingDataId, 'TDID, 'Date).orderBy('rand.asc)

    val positivePool = candidatePool
      .filter('TargetingDataId.isNotNull)
      .withColumn("rand", rand())
      .withColumn("row", row_number().over(window)).cache()

    val positiveSample = generatePositiveSample(positivePool)
    val softNegativeSample = generateSoftNegativeSample(positiveSample, candidatePool)
    val hardNegativeSample = generateHardNegativeSample(positivePool, positiveSample, candidatePool)

    FirstPartyPixelModelInputDataset().writePartition(
      ModelFeatureTransform.modelFeatureTransform[FirstPartyPixelModelInputRecord](positiveSample),
      date,
      subFolderKey = Some(subFolder),
      subFolderValue = Some("positive"),
      format = Some("tfrecord")
    )
    positiveSampledCount.set(positiveSample.count())

    FirstPartyPixelModelInputDataset().writePartition(
      ModelFeatureTransform.modelFeatureTransform[FirstPartyPixelModelInputRecord](softNegativeSample),
      date,
      subFolderKey = Some(subFolder),
      subFolderValue = Some("softNegative"),
      format = Some("tfrecord")
    )
    softNegativeSampledCount.set(softNegativeSample.count())

    FirstPartyPixelModelInputDataset().writePartition(
      ModelFeatureTransform.modelFeatureTransform[FirstPartyPixelModelInputRecord](hardNegativeSample),
      date,
      subFolderKey = Some(subFolder),
      subFolderValue = Some("hardNegative"),
      format = Some("tfrecord")
    )
    hardNegativeSampledCount.set(hardNegativeSample.count())

    prometheus.pushMetrics()
  }

  def generatePositiveSample(positivePool: DataFrame): DataFrame = {
    // to control
    val window1 = Window.partitionBy('TargetingDataId, 'Date).orderBy('rand.asc)
    // restrict the number of records for each tdid on every day and the max positive record per day
    val positiveSample = positivePool
      .filter('row <= numTDID)
      .withColumn("pos_row", row_number().over(window1))
      .filter('pos_row <= maxPos)
      .drop("row", "rand", "pos_row")
      .withColumn("Target", lit(1.0))
      .cache()

    positiveSample
  }

  def generateSoftNegativeSample(positiveSample: DataFrame, candidatePool: DataFrame): DataFrame = {
    // use for calculating the negative samples number
    val positiveStats = positiveSample
      .groupBy("TargetingDataId", "CampaignId", "Date")
      .agg(count("TDID") as "NumPos")

    // generate negative samples
    val negativePool = candidatePool
      .filter('TargetingDataId.isNull)
      .withColumn("rand", rand()) // use for negative samples generation
      .drop("TargetingDataId")
      .cache()

    val negativeStats = negativePool
      .groupBy( "CampaignId", "Date")
      .agg(count("TDID") as "NumNeg")

    // save the information for TargetingDataId + campaignid; how many bidimp should be sampled
    val negativeStats1 = negativeStats
      .join(positiveStats,
        Seq("CampaignId", "Date"),
        "inner")
      // rate for random sample
      .withColumn("Rate", lit(negFactor) * 'NumPos / 'NumNeg)
      // in case we do not have enough neg samples
      .withColumn("SampleRate", when('Rate >= 1, lit(1)).otherwise('Rate))
      // add more diversity into the sampling accross different campaigns
      .withColumn("LB", (lit(1) - 'SampleRate) * rand())
      .withColumn("UB", 'LB + 'SampleRate)
      .drop("Rate", "SampleRate", "NumNeg", "NumPos")
      .cache()

    val negativeSample = negativePool
      .join(broadcast(negativeStats1),
        Seq("CampaignId",
          "Date"),
        "inner")
      .filter(
        $"rand" >= $"LB" &&
          $"rand" <= $"UB")
      .drop("rand", "UB", "LB")
      .withColumn("Target", lit(0.0))
      .cache()

    negativeSample
  }

  def generateHardNegativeSample(positivePool : DataFrame, positiveSample: DataFrame, candidatePool: DataFrame): DataFrame = {
    val trackingTagLevelPool = candidatePool
      .select("TargetingDataId",
        "CampaignId",
        "TDID")
      .withColumn("T", lit(1))
      .distinct()

    val campaignLevelPool = candidatePool
      .select("CampaignId",
        "TDID")
      .distinct()

    val campaignTrackingTags = candidatePool
      .select("TargetingDataId",
        "CampaignId")
      .distinct()

    val fullPool = campaignLevelPool
      .join(campaignTrackingTags, "CampaignId")

    val hardNegPool = fullPool
      .join(trackingTagLevelPool,
        Seq("TargetingDataId", "CampaignId", "TDID"),
        "left")
      .filter('T.isNull)
      .drop("T")

    val positiveStats = positiveSample
      .groupBy("TargetingDataId", "CampaignId")
      .agg(count("TDID") as "NumPos")

    val hardNegStats = hardNegPool
      .groupBy("TargetingDataId", "CampaignId")
      .agg(count("TDID").as("NumNeg"))
      .join(positiveStats,
        Seq("TargetingDataId", "CampaignId"),
        "inner")
      // narrow down to 4*numTDID records per tdid
      .withColumn("Rate", lit(negFactor) * 'NumPos / 'NumNeg / lit(5 * numTDID))
      .withColumn("SampleRate", when('Rate >= 1, lit(1)).otherwise('Rate))
      .select("TargetingDataId",
        "CampaignId",
        "SampleRate")

    val sampledHardNegPool = hardNegPool
      .join(hardNegStats,
        Seq("TargetingDataId", "CampaignId"),
        "inner")
      .withColumn("LB", (lit(1)-'SampleRate)*rand())
      .withColumn("UB", 'LB + 'SampleRate)
      .drop("SampleRate")

    // hard negative sample combines seem and unseem positive impressions; currently set as 1:4
    val sampledPositivePool = positivePool
      .filter('row <= 5 * numTDID)
      .drop("TargetingDataId", "CampaignId")

    val hardNegSample = sampledPositivePool
      .join(sampledHardNegPool,
        Seq("TDID"),
        "inner")
      .filter($"rand" >= $"LB" &&
        $"rand" <= $"UB")
      .drop("rand", "UB", "LB", "row")
      .withColumn("Target", lit(0.0))
      .cache()

    hardNegSample
  }
}
