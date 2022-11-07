package com.thetradedesk.audience.jobs

import com.thetradedesk.audience.datasets.{FirstPartyPixelModelInputDataset, FirstPartyPixelModelInputRecord, SeenInBiddingV3DeviceDataSet, TargetingDataDataset, TrackingTagDataset, UniversalPixelDataset, UniversalPixelTrackingTagDataset}
import com.thetradedesk.audience.{date, sampleHit, trainSetDownSampleFactor}
import com.thetradedesk.audience.sample.DownSample.hashSampleV2
import com.thetradedesk.audience.transform.{ModelFeatureTransform,FirstPartyDataTransform}
import com.thetradedesk.geronimo.bidsimpression.schema.{BidsImpressions, BidsImpressionsSchema}
import com.thetradedesk.geronimo.shared.{GERONIMO_DATA_SOURCE, loadParquetData}
import com.thetradedesk.spark.sql.SQLFunctions._
import com.thetradedesk.spark.TTDSparkContext.spark
import com.thetradedesk.spark.TTDSparkContext.spark.implicits._
import com.thetradedesk.spark.util.HashingUtils.userIsInSample
import com.thetradedesk.spark.util.prometheus.PrometheusClient
import com.thetradedesk.spark.util.TTDConfig.{config, defaultCloudProvider}
import java.time.LocalDate
import org.apache.spark.sql.{DataFrame, Dataset, SaveMode}
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._

object FirstPartyPixelModelDailySIBSampleGeneration {
  private val bidsImpressionLookBack = config.getInt("bidsImpressionLookBack", 5)
  private val tdidSampleMod = config.getInt("tdidSampleMod", 1)
  private val subFolder = config.getString("subFolder", "split")
  private val numTDID = config.getInt("numTDID", 100)
  // control maximum positive records for a pxiel on given day; without it, the data size will be huge
  private val maxPositiveSamplesPerSegment = config.getInt("maxPositiveSamplesPerSegment", default = 6000)
  // used for the hard negative samples generation
  private val softNegFactor = config.getInt("softNegFactor", default = 10)
  private val hardNegFactor = config.getInt("hardNegFactor", default = 10)
  private val partitionCount = config.getInt("partitionCount", default = 120)
  private val selectedPixelsConfigPath = config.getString("selectedPixelsConfigPath", "s3a://thetradedesk-useast-hadoop/Data_Science/Yang/audience_extension/selected_1pp/firstPixel200_TargetingDataId/")

  val userIsInSampleUDF = udf[Boolean, String, Long, Long](userIsInSample)
  val doNotTrackTDID = lit("00000000-0000-0000-0000-000000000000")

  def shouldConsiderTDID(symbol: Symbol) = {
    symbol.isNotNullOrEmpty && symbol =!= doNotTrackTDID && substring(symbol, 9, 1) === lit("-") && userIsInSampleUDF(symbol, lit(1000000), lit(10000))
  }

  def main(args: Array[String]): Unit = {
    val prometheus = new PrometheusClient("FirstPartyPixelModel", "DailySIBSampleGeneration")
    val trainingSampledCount = prometheus.createGauge("training_sample_count", "Number of records in the training set")
    val validationSampledCount = prometheus.createGauge("validation_sample_count", "Number of records in the validation set")

    val (trainingSet, validationSet) = runETLPipeline(date)

    validationSet.cache()
    FirstPartyPixelModelInputDataset("dailySIBSample").writePartition(
      validationSet,
      date,
      subFolderKey = Some(subFolder),
      subFolderValue = Some("val_tfrecord"),
      format = Some("tfrecord"),
      saveMode = SaveMode.Overwrite
    )
    validationSampledCount.set(validationSet.count())
    validationSet.unpersist()

    val hashedTrainingSet = trainingSet.withColumn("TDIDHash", abs(hash(concat('TDID, 'TargetingDataId))))
    hashedTrainingSet.cache()
    FirstPartyPixelModelInputDataset("dailySIBSample").writePartition(
      hashedTrainingSet.filter('TDIDHash % trainSetDownSampleFactor === lit(sampleHit)).drop("TDIDHash").as[FirstPartyPixelModelInputRecord],
      date,
      subFolderKey = Some(subFolder),
      subFolderValue = Some("train_tfrecord"),
      format = Some("tfrecord"),
      saveMode = SaveMode.Overwrite
    )
    FirstPartyPixelModelInputDataset("dailySIBSample").writePartition(
      hashedTrainingSet.filter('TDIDHash % trainSetDownSampleFactor =!= lit(sampleHit)).drop("TDIDHash").as[FirstPartyPixelModelInputRecord],
      date,
      subFolderKey = Some(subFolder),
      subFolderValue = Some("holdout_tfrecord"),
      format = Some("tfrecord"),
      saveMode = SaveMode.Overwrite
    )
    trainingSampledCount.set(trainingSet.count())
    trainingSet.unpersist()

    prometheus.pushMetrics()
  }

  def runETLPipeline(date: LocalDate) = {
    val bidImpressionsS3Path = BidsImpressions.BIDSIMPRESSIONSS3 + "prod/bidsimpressions/"
    val bidsImpressionsLong = loadParquetData[BidsImpressionsSchema](bidImpressionsS3Path, date, source = Some(GERONIMO_DATA_SOURCE), lookBack=Some(bidsImpressionLookBack))
      .withColumnRenamed("UIID", "TDID")
      .filter(shouldConsiderTDID('TDID))
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
      .withColumn("AdWidthInPixels", ('AdWidthInPixels - lit(1.0))/lit(9999.0)) // 1 - 10000
      .withColumn("AdWidthInPixels", when('AdWidthInPixels.isNotNull, 'AdWidthInPixels).otherwise(0))
      .withColumn("AdHeightInPixels", ('AdHeightInPixels - lit(1.0))/lit(9999.0)) // 1 - 10000
      .withColumn("AdHeightInPixels", when('AdHeightInPixels.isNotNull, 'AdHeightInPixels).otherwise(0))
      .withColumn("Latitude", ('Latitude + lit(90.0))/lit(180.0))  // -90 - 90
      .withColumn("Latitude", when('Latitude.isNotNull, 'Latitude).otherwise(0))
      .withColumn("Longitude", ('Longitude + lit(180.0))/lit(360.0)) //-180 - 180
      .withColumn("Longitude", when('Longitude.isNotNull, 'Longitude).otherwise(0))

    val seenInBiddingDeviceDataset = SeenInBiddingV3DeviceDataSet().readPartition(date)(spark)
      .withColumnRenamed("DeviceId", "TDID")
      .filter(size('ThirdPartyTargetingDataIds) > 0 || size('FirstPartyTargetingDataIds) > 0)
      .withColumn("TargetingDataIds", array_union('ThirdPartyTargetingDataIds, 'FirstPartyTargetingDataIds))
      .select('TDID, 'TargetingDataIds)

    val sampledBidsImpressionsKeys = loadParquetData[BidsImpressionsSchema](bidImpressionsS3Path, date, source = Some(GERONIMO_DATA_SOURCE), lookBack=Some(bidsImpressionLookBack))
      .withColumnRenamed("UIID", "TDID")
      .withColumn("Date", to_date('LogEntryTime, "yyyy-MM-dd"))
      .select('BidRequestId, 'Date, 'TDID, 'CampaignId)
      .filter(shouldConsiderTDID('TDID))  // in the future, we may not have the id, good to think about how to solve

    val allTargetingDataIds = spark.sparkContext.broadcast(spark.read.parquet(selectedPixelsConfigPath).as[Long].collect)

    val selectedSIB = seenInBiddingDeviceDataset
      .select('TDID, array_intersect('TargetingDataIds, typedLit(allTargetingDataIds.value)).as("TargetingDataIds"))
      .filter(size('TargetingDataIds) > 0)
      .cache()

    val validationSet = ModelFeatureTransform.modelFeatureTransform[FirstPartyPixelModelInputRecord](generateDataSet(
      sampledBidsImpressionsKeys.filter('Date >= date),
      bidsImpressionsLong,
      selectedSIB
    ))

    val trainingSet = ModelFeatureTransform.modelFeatureTransform[FirstPartyPixelModelInputRecord](generateDataSet(
      sampledBidsImpressionsKeys.filter('Date < date),
      bidsImpressionsLong,
      selectedSIB
    ))

    (trainingSet, validationSet)
  }

  def generateDataSet(bidsImpressions: DataFrame, bidsImpressionsLong: DataFrame, selectedSIB: DataFrame) = {
    val window = Window.partitionBy('TargetingDataId, 'TDID, 'Date).orderBy('rand.asc)

    val positivePool = bidsImpressions
      .join(selectedSIB, Seq("TDID"), "inner")
      .withColumn("TargetingDataId", explode('TargetingDataIds)).drop("TargetingDataIds")
      .withColumn("rand", rand())
      .withColumn("row", rank().over(window))
      .cache()

    val softNegativePool = bidsImpressions.join(selectedSIB, Seq("TDID"), "left_anti")

    val positiveSample = generatePositiveSample(positivePool).cache
    val negativeSample = generateSoftNegativeSample(positiveSample, softNegativePool)
    val hardNegativeSample = generateHardNegativeSample(positivePool, positiveSample, bidsImpressions)
    positivePool.unpersist()

    positiveSample
      .unionByName(negativeSample)
      .unionByName(hardNegativeSample)
      .join(bidsImpressionsLong, Seq("BidRequestId"), "inner")
  }

  def generatePositiveSample(positivePool: DataFrame): DataFrame = {
    // to control
    val window1 = Window.partitionBy('TargetingDataId, 'Date).orderBy('rand.asc)
    // restrict the number of records for each tdid on every day and the max positive record per day
    val positiveSample = positivePool
      .filter('row <= numTDID)
      .withColumn("pos_row", row_number().over(window1))
      .filter('pos_row <= maxPositiveSamplesPerSegment)
      .drop("row", "rand", "pos_row")
      .withColumn("Target", lit(1.0))

    positiveSample
  }

  def generateSoftNegativeSample(positiveSample: DataFrame, softNegativePool: DataFrame): DataFrame = {
    // use for calculating the negative samples number
    val positiveStats = positiveSample
      .groupBy("TargetingDataId", "CampaignId", "Date")
      .agg(count("TDID") as "NumPos")

    // generate negative samples
    val negativePool = softNegativePool
      .withColumn("rand", rand()) // use for negative samples generation
      .cache()

    val negativeStats = negativePool
      .groupBy( "CampaignId", "Date")
      .agg(count("TDID") as "NumNeg")

    // save the information for TargetingDataId + campaignid; how many bidimp should be sampled
    val negativeStats1 = negativeStats
      .join(positiveStats, Seq("CampaignId", "Date"), "inner")
      // rate for random sample
      .withColumn("Rate", lit(softNegFactor) * 'NumPos / 'NumNeg)
      // in case we do not have enough neg samples
      .withColumn("Rate", when('Rate >= 1, lit(1)).otherwise('Rate))
      .drop("NumNeg", "NumPos")

    val negativeSample = negativePool
      .join(broadcast(negativeStats1), Seq("CampaignId", "Date"), "inner")
      .filter($"rand" <= $"Rate")
      .drop("rand", "Rate")
      .withColumn("Target", lit(0.0))

    negativeSample
  }

  def generateHardNegativeSample(positivePool : DataFrame, positiveSample: DataFrame, sampledBidsImpressions: DataFrame): DataFrame = {
    val trackingTagLevelPool = positivePool
      .select("TargetingDataId", "CampaignId", "TDID")
      .distinct()

    val campaignLevelPool = positivePool
      .select("CampaignId", "TDID")
      .distinct()

    val campaignTrackingTags = positivePool
      .select("TargetingDataId", "CampaignId")
      .distinct()

    val fullPool = campaignLevelPool
      .join(campaignTrackingTags, "CampaignId")

    val hardNegPool = fullPool
      .join(trackingTagLevelPool, Seq("TargetingDataId", "CampaignId", "TDID"), "left_anti")

    // get the distinct positive impression of the tdid
    val distinctPosPool = sampledBidsImpressions
      .join(campaignLevelPool, Seq("TDID", "CampaignId"), "inner")

    // generate the impression pool of the hard negative
    val hardNegImp = hardNegPool
      .join(distinctPosPool, Seq("CampaignId", "TDID"))
      .withColumn("rand", rand()) // for the same tdid in the same of different campaign they will have their own random number
      .cache

    // calculate how many positive samples and determine how many negative samples we should have
    val positiveStats = positiveSample
      .groupBy("TargetingDataId", "CampaignId")
      .agg(count("TDID") as "NumPos")

    val hardNegStats = hardNegImp
      .groupBy("TargetingDataId", "CampaignId")
      .agg(count("TDID").as("NumNeg"))
      .join(positiveStats, Seq("TargetingDataId", "CampaignId"), "inner")
      .withColumn("Rate", lit(hardNegFactor) * 'NumPos / 'NumNeg)
      .withColumn("SampleRate", when('Rate >= 1, lit(1)).otherwise('Rate))
      .select("TargetingDataId", "CampaignId", "SampleRate")

    val hardNegSample = hardNegImp
      .join(hardNegStats, Seq("TargetingDataId", "CampaignId"), "inner")
      .filter($"rand" <= $"SampleRate")
      .drop("rand", "SampleRate")
      .withColumn("Target", lit(0.0))

    hardNegSample
  }
}
