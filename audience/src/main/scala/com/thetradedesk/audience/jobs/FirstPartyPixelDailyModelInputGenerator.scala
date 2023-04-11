package com.thetradedesk.audience.jobs

import com.thetradedesk.audience.datasets.{ConversionDataset, FirstPartyPixelModelInputDataset, FirstPartyPixelModelInputRecord, LightCrossDeviceGraphDataset, LightTrackingTagDataset, SampledCrossDeviceGraphDataset, SeenInBiddingV3DeviceDataSet, TargetingDataDataset, UniversalPixelDataset, UniversalPixelTrackingTagDataset}
import com.thetradedesk.audience.{date, sampleHit, shouldConsiderTDID, shouldConsiderTDID2, trainSetDownSampleFactor}
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

abstract class FirstPartyPixelDailyModelInputGenerator {
  val samplingFunction = shouldConsiderTDID _

  object Config {
    var bidsImpressionLookBack = config.getInt("bidsImpressionLookBack", 5)
    var numTDID = config.getInt("numTDID", 100)
    // control maximum positive records for a pxiel on given day; without it, the data size will be huge
    var maxPositiveSamplesPerSegment = config.getInt("maxPositiveSamplesPerSegment", default = 0)
    // used for the hard negative samples generation
    var softNegFactor = config.getDouble("softNegFactor", default = 10.0)
    var hardNegFactor = config.getDouble("hardNegFactor", default = 10.0)
    var persistHoldoutSet = config.getBoolean("persistHoldoutSet", default = false)
    var useCrossDeviceGraph = config.getBoolean("useCrossDeviceGraph", default = false)
    var trainSetDownSampleKeysRaw = config.getString("trainSetDownSampleKeys", default = "TDID,TargetingDataId")
    var selectedPixelsConfigPath = config.getString("selectedPixelsConfigPath", "s3a://thetradedesk-useast-hadoop/Data_Science/freeman/audience_extension/firstPixel46_TargetingDataId/")
    lazy val trainSetDownSampleKeys = trainSetDownSampleKeysRaw.split(",")

    var subFolder = config.getString("subFolder", "split")
    var datasetName = config.getString("datasetName", "dailySIBSample")
    var datasetVersion = config.getInt("datasetVersion", 1)
    var useDownSampledDeviceGraph = config.getBoolean("useDownSampledDeviceGraph", default = true)
  }

  def main(args: Array[String]): Unit = {
    val prometheus = new PrometheusClient("FirstPartyPixelModel", "DailySIBSampleGeneration")
    val trainingSampledCount = prometheus.createGauge("training_sample_count", "Number of records in the training set")
    val validationSampledCount = prometheus.createGauge("validation_sample_count", "Number of records in the validation set")

    val (trainingSet, validationSet) = runETLPipeline(date)

    validationSet.cache()
    FirstPartyPixelModelInputDataset(Config.datasetName, Config.datasetVersion).writePartition(
      validationSet.filter('IsPrimaryTDID === lit(1)),
      date,
      subFolderKey = Some(Config.subFolder),
      subFolderValue = Some("val_tfrecord"),
      format = Some("tfrecord"),
      saveMode = SaveMode.Overwrite
    )
    FirstPartyPixelModelInputDataset(Config.datasetName + "CrossDeviceGraphExtension", Config.datasetVersion).writePartition(
      validationSet.filter('IsPrimaryTDID =!= lit(1)),
      date,
      subFolderKey = Some(Config.subFolder),
      subFolderValue = Some("val_tfrecord"),
      format = Some("tfrecord"),
      saveMode = SaveMode.Overwrite
    )
    validationSampledCount.set(validationSet.count())
    validationSet.unpersist()

    val hashedTrainingSet = trainingSet.withColumn("TDIDHash", abs(hash(concat(Config.trainSetDownSampleKeys.map(col(_)): _*))))
    hashedTrainingSet.cache()
    FirstPartyPixelModelInputDataset(Config.datasetName, Config.datasetVersion).writePartition(
      hashedTrainingSet.filter('TDIDHash % trainSetDownSampleFactor === lit(sampleHit) && 'IsPrimaryTDID === lit(1)).drop("TDIDHash").as[FirstPartyPixelModelInputRecord],
      date,
      subFolderKey = Some(Config.subFolder),
      subFolderValue = Some("train_tfrecord"),
      format = Some("tfrecord"),
      saveMode = SaveMode.Overwrite
    )
    FirstPartyPixelModelInputDataset(Config.datasetName + "CrossDeviceGraphExtension", Config.datasetVersion).writePartition(
      hashedTrainingSet.filter('TDIDHash % trainSetDownSampleFactor === lit(sampleHit) && 'IsPrimaryTDID =!= lit(1)).drop("TDIDHash").as[FirstPartyPixelModelInputRecord],
      date,
      subFolderKey = Some(Config.subFolder),
      subFolderValue = Some("train_tfrecord"),
      format = Some("tfrecord"),
      saveMode = SaveMode.Overwrite
    )

    if (Config.persistHoldoutSet) {
      FirstPartyPixelModelInputDataset(Config.datasetName, Config.datasetVersion).writePartition(
        hashedTrainingSet.filter('TDIDHash % trainSetDownSampleFactor =!= lit(sampleHit) && 'IsPrimaryTDID === lit(1)).drop("TDIDHash").as[FirstPartyPixelModelInputRecord],
        date,
        subFolderKey = Some(Config.subFolder),
        subFolderValue = Some("holdout_tfrecord"),
        format = Some("tfrecord"),
        saveMode = SaveMode.Overwrite
      )
      FirstPartyPixelModelInputDataset(Config.datasetName + "CrossDeviceGraphExtension", Config.datasetVersion).writePartition(
        hashedTrainingSet.filter('TDIDHash % trainSetDownSampleFactor =!= lit(sampleHit) && 'IsPrimaryTDID =!= lit(1)).drop("TDIDHash").as[FirstPartyPixelModelInputRecord],
        date,
        subFolderKey = Some(Config.subFolder),
        subFolderValue = Some("holdout_tfrecord"),
        format = Some("tfrecord"),
        saveMode = SaveMode.Overwrite
      )
    }

    trainingSampledCount.set(trainingSet.count())
    trainingSet.unpersist()

    prometheus.pushMetrics()
  }

  def addCrossDeviceData(date: LocalDate, labels: DataFrame) = {
    if (!Config.useCrossDeviceGraph) {
      labels.withColumn("isPrimaryTDID", lit(1))
    } else {
      val graph = SampledCrossDeviceGraphDataset().readPartition(date, lookBack = Some(6))(spark)

      val sampledGraph = CrossDeviceGraphSampler.downSampleGraph(graph, samplingFunction).drop("deviceType")

      val graphByPerson = sampledGraph.groupBy("personID").agg(collect_set("TDID").as("TDIDs"))

      // apply it on the conversion list to add the personID on it
      val labelsWithGraph = labels.join(sampledGraph, Seq("TDID"), "left")
        .withColumnRenamed("TDID", "TDID_original")
        .join(graphByPerson, Seq("personID"), "left")
        .withColumn("TDID", explode_outer('TDIDs)).drop("TDIDs")
        .withColumn("TDID", when('TDID.isNotNull, 'TDID).otherwise('TDID_original))
        .withColumn("isPrimaryTDID", when('TDID_original === 'TDID, lit(1)).otherwise(lit(0)))
        .drop("TDID_original", "personID")

      if (labelsWithGraph.columns.contains("TargetingDataIds")) {
        val flatten_distinct = (array_distinct _) compose (flatten _)
        val aggregatedSegments = labelsWithGraph.groupBy('TDID, 'isPrimaryTDID).agg(flatten_distinct(collect_set('TargetingDataIds)).as("TargetingDataIds"))
        val mutuallyExclusiveSegments = aggregatedSegments.filter('isPrimaryTDID === lit(1)).withColumnRenamed("TargetingDataIds", "yes").select("TDID", "yes")
          .join(aggregatedSegments.filter('isPrimaryTDID =!= lit(1)).withColumnRenamed("TargetingDataIds", "no").select("TDID", "no"), Seq("TDID"), "outer")
          .withColumn("yes", when('yes.isNotNull, 'yes).otherwise(typedLit(Seq[BigInt]())))
          .withColumn("no", when('no.isNotNull, 'no).otherwise(typedLit(Seq[BigInt]())))
          .withColumn("no", array_except('no, 'yes))

        mutuallyExclusiveSegments.select("TDID", "yes").withColumnRenamed("yes", "TargetingDataIds").withColumn("isPrimaryTDID", lit(1))
          .unionByName(mutuallyExclusiveSegments.select("TDID", "no").withColumnRenamed("no", "TargetingDataIds").withColumn("isPrimaryTDID", lit(0)))
          .filter('TargetingDataIds.isNotNull && size('TargetingDataIds) > 0)
      } else {
        labelsWithGraph.groupBy('TargetingDataId, 'TDID).agg(max('isPrimaryTDID).as('isPrimaryTDID))
      }
    }
  }

  def getLabels(date: LocalDate): DataFrame

  def getBidImpressions(date: LocalDate) = {
    val bidImpressionsS3Path = BidsImpressions.BIDSIMPRESSIONSS3 + "prod/bidsimpressions/"
    val bidsImpressionsLong = loadParquetData[BidsImpressionsSchema](bidImpressionsS3Path, date, source = Some(GERONIMO_DATA_SOURCE), lookBack=Some(Config.bidsImpressionLookBack))
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
        'sin_hour_week,  // time based features sometime are useful than expected
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
        'LogEntryTime
      )
      // they saved in struct type
      .withColumn("OperatingSystemFamily", 'OperatingSystemFamily("value"))
      .withColumn("Browser", 'Browser("value"))
      .withColumn("RenderingContext", 'RenderingContext("value"))
      .withColumn("InternetConnectionType", 'InternetConnectionType("value"))
      .withColumn("OperatingSystem", 'OperatingSystem("value"))
      .withColumn("DeviceType", 'DeviceType("value"))
      .withColumn("AdWidthInPixels", ('AdWidthInPixels - lit(1.0))/lit(9999.0)) // 1 - 10000
      .withColumn("AdWidthInPixels", when('AdWidthInPixels.isNotNull, 'AdWidthInPixels).otherwise(0))
      .withColumn("AdHeightInPixels", ('AdHeightInPixels - lit(1.0))/lit(9999.0)) // 1 - 10000
      .withColumn("AdHeightInPixels", when('AdHeightInPixels.isNotNull, 'AdHeightInPixels).otherwise(0))
      .withColumn("Latitude", ('Latitude + lit(90.0))/lit(180.0))  // -90 - 90
      .withColumn("Latitude", when('Latitude.isNotNull, 'Latitude).otherwise(0))
      .withColumn("Longitude", ('Longitude + lit(180.0))/lit(360.0)) //-180 - 180
      .withColumn("Longitude", when('Longitude.isNotNull, 'Longitude).otherwise(0))

    val sampledBidsImpressionsKeys = bidsImpressionsLong
      .withColumn("Date", to_date('LogEntryTime, "yyyy-MM-dd"))
      .select('BidRequestId, 'Date, 'TDID, 'CampaignId)
      .filter(samplingFunction('TDID))  // in the future, we may not have the id, good to think about how to solve

    (sampledBidsImpressionsKeys, bidsImpressionsLong)
  }

  def runETLPipeline(date: LocalDate) = {
    val (sampledBidsImpressionsKeys, bidsImpressionsLong) = getBidImpressions(date)

    val selectedSIB = getLabels(date)

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

  def generateDataSet(bidsImpressions: DataFrame, bidsImpressionsLong: DataFrame, labels: DataFrame) = {

    val (positivePool, softNegativePool) = if (labels.columns.contains("TargetingDataIds")) {
      // SIB
      val positivePool = bidsImpressions.join(labels, Seq("TDID"), "inner")
        .withColumn("TargetingDataId", explode('TargetingDataIds)).drop("TargetingDataIds")
        .cache
      val softNegativePool = bidsImpressions.join(labels.dropDuplicates("TDID"), Seq("TDID"), "left_anti")
        .withColumn("isPrimaryTDID", lit(1))
      (positivePool, softNegativePool)
    } else {
      // Conversion Tracker
      val candidatePool = bidsImpressions.join(labels, Seq("TDID"), "left")
        .cache
      val positivePool = candidatePool.filter('TargetingDataId.isNotNull)
        .cache
      val softNegativePool = candidatePool.filter('TargetingDataId.isNull)
        .withColumn("isPrimaryTDID", lit(1))

      (positivePool, softNegativePool)
    }

    val positiveSample = generatePositiveSample(positivePool).cache
    val negativeSample = generateSoftNegativeSample(positiveSample, softNegativePool)
    val hardNegativeSample = generateHardNegativeSample(positivePool, positiveSample, bidsImpressions)

    positiveSample
      .unionByName(negativeSample)
      .unionByName(hardNegativeSample)
      .join(bidsImpressionsLong, Seq("BidRequestId"), "inner")
  }

  def generatePositiveSample(positivePool: DataFrame): DataFrame = {
    val window = Window.partitionBy('TargetingDataId, 'TDID, 'Date).orderBy('rand.asc)
    val window1 = Window.partitionBy('TargetingDataId, 'Date).orderBy('rand.asc)

    // restrict the number of records for each tdid on every day and the max positive record per day
    var positiveSample = positivePool
      .withColumn("rand", rand())
      .withColumn("row", rank().over(window))
      .filter('row <= Config.numTDID)

    if (Config.maxPositiveSamplesPerSegment > 0) {
      positiveSample = positiveSample.withColumn("pos_row", row_number().over(window1))
        .filter('pos_row <= Config.maxPositiveSamplesPerSegment)
        .drop("pos_row")
    }

    positiveSample.drop("row", "rand").withColumn("Target", lit(1.0))
  }

  def generateSoftNegativeSample(positiveSample: DataFrame, softNegativePool: DataFrame): DataFrame = {
    // use for calculating the negative samples number
    val positiveStats = positiveSample
      .groupBy("TargetingDataId", "CampaignId", "Date")
      .agg(count("TDID") as "NumPos")

    // generate negative samples
    val negativePool = softNegativePool
      .withColumn("rand", rand()) // use for negative samples generation
      .drop("TargetingDataId")
      .cache()

    val negativeStats = negativePool
      .groupBy( "CampaignId", "Date")
      .agg(count("TDID") as "NumNeg")

    // save the information for TargetingDataId + campaignid; how many bidimp should be sampled
    val negativeStats1 = negativeStats
      .join(positiveStats, Seq("CampaignId", "Date"), "inner")
      // rate for random sample
      .withColumn("Rate", lit(Config.softNegFactor) * 'NumPos / 'NumNeg)
      // in case we do not have enough neg samples
      .withColumn("Rate", when('Rate >= 1, lit(1)).otherwise('Rate))
      .drop("NumNeg", "NumPos")

    val negativeSample = negativePool
      .join(broadcast(negativeStats1), Seq("CampaignId", "Date"), "inner")
      .filter($"rand" < $"Rate")
      .drop("rand", "Rate")
      .withColumn("Target", lit(0.0))

    negativeSample
  }

  def generateHardNegativeSample(positivePool : DataFrame, positiveSample: DataFrame, sampledBidsImpressions: DataFrame): DataFrame = {
    val trackingTagLevelPool = positivePool
      .select("TargetingDataId", "CampaignId", "TDID", "IsPrimaryTDID")
      .distinct()

    val campaignLevelPool = positivePool
      .select("CampaignId", "TDID", "IsPrimaryTDID")
      .distinct()

    val campaignTrackingTags = positivePool
      .select("TargetingDataId", "CampaignId", "IsPrimaryTDID")
      .distinct()

    val fullPool = campaignLevelPool
      .join(campaignTrackingTags, Seq("CampaignId", "IsPrimaryTDID"))

    val hardNegPool = fullPool
      .join(trackingTagLevelPool, Seq("TargetingDataId", "IsPrimaryTDID", "CampaignId", "TDID"), "left_anti")

    // get the distinct positive impression of the tdid
    val distinctPosPool = sampledBidsImpressions
      .join(campaignLevelPool, Seq("TDID", "CampaignId"), "inner")

    // generate the impression pool of the hard negative
    val hardNegImp = hardNegPool
      .join(distinctPosPool, Seq("CampaignId", "TDID", "IsPrimaryTDID"))
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
      .withColumn("Rate", lit(Config.hardNegFactor) * 'NumPos / 'NumNeg)
      .withColumn("SampleRate", when('Rate >= 1, lit(1)).otherwise('Rate))
      .select("TargetingDataId", "CampaignId", "SampleRate")

    val hardNegSample = hardNegImp
      .join(hardNegStats, Seq("TargetingDataId", "CampaignId"), "inner")
      .filter($"rand" < $"SampleRate")
      .drop("rand", "SampleRate")
      .withColumn("Target", lit(0.0))

    hardNegSample
  }
}

object FirstPartyPixelDailySIBModelInputGenerator extends FirstPartyPixelDailyModelInputGenerator {
  object SIBConfig {
    var seenInBiddingLookBack = config.getInt("seenInBiddingLookBack", default=6)
  }

  def readSeenInBiddingDay(date: LocalDate, allTargetingDataIds: org.apache.spark.broadcast.Broadcast[_]): DataFrame = {
    SeenInBiddingV3DeviceDataSet().readPartition(date)(spark)
      .withColumnRenamed("DeviceId", "TDID")
      .filter(size('FirstPartyTargetingDataIds) > 0)
      .withColumn("TargetingDataIds", array_intersect('FirstPartyTargetingDataIds, typedLit(allTargetingDataIds.value)))
      .select('TDID, 'TargetingDataIds)
      .filter(size('TargetingDataIds) > 0)
  }

  def getLabels(date: LocalDate): DataFrame = {
    val allTargetingDataIds = spark.sparkContext.broadcast(spark.read.parquet(Config.selectedPixelsConfigPath).as[Long].collect)

    var selectedSIB = readSeenInBiddingDay(date, allTargetingDataIds).withColumn("daysAgo", lit(0))

    for (i <- 1 to SIBConfig.seenInBiddingLookBack) {
      selectedSIB = selectedSIB.unionByName(readSeenInBiddingDay(date.minusDays(i), allTargetingDataIds).withColumn("daysAgo", lit(i)))
    }

    selectedSIB = selectedSIB
      .withColumn("rank", rank().over(Window.partitionBy('TDID).orderBy('daysAgo)))
      .filter('rank === lit(1)).drop("rank", "daysAgo")
      .cache()

    addCrossDeviceData(date, selectedSIB)
  }
}

object FirstPartyPixelDailyConversionModelInputGenerator extends FirstPartyPixelDailyModelInputGenerator {
  override val samplingFunction = shouldConsiderTDID2 _

  object ConversionConfig {
    var conversionLookBack = config.getInt("conversionLookBack", 1)
  }

  def getLabels(date: LocalDate): DataFrame = {
    val trackingTagDataset = LightTrackingTagDataset().readPartition(date)
      .select("TrackingTagId", "TargetingDataId")

    val allTargetingDataIds = spark.read.parquet(Config.selectedPixelsConfigPath)
      .join(trackingTagDataset, Seq("TargetingDataId"), "inner")

    val conversionDS = ConversionDataset(defaultCloudProvider).readRange(date.minusDays(ConversionConfig.conversionLookBack).atStartOfDay(), date.atStartOfDay())
      .select('TDID, 'TrackingTagId)
      .join(broadcast(allTargetingDataIds), Seq("TrackingTagId"), "inner")
      .select('TargetingDataId, 'TDID)
      .distinct()

    addCrossDeviceData(date, conversionDS)
  }
}

object CrossDeviceGraphSampler {
  object Config {
    var graphOutputPath = config.getString("graphOutputPath", "")
  }

  private val dateFormatter = DateTimeFormatter.ofPattern("yyyyMMdd")

  def downSampleGraph(graph: Dataset[_], samplingFunction: (Symbol => Column)) = {
    val sibSampledPersonIDs = graph
      .filter(samplingFunction('TDID))
      .select("personID")

    graph.join(sibSampledPersonIDs, Seq("personID"), "inner")
  }

  def main(args: Array[String]): Unit = {
    for (i <- 0 to 6) {
      val sourcePath = s"${LightCrossDeviceGraphDataset().basePath}/${date.minusDays(i).format(LightCrossDeviceGraphDataset().crossDeviceDateFormatter)}"
      val destPath = Config.graphOutputPath + "/date=" + date.minusDays(i).format(DateTimeFormatter.ofPattern("yyyyMMdd"))

      if (FSUtils.directoryExists(sourcePath)(spark) && FSUtils.directoryExists(destPath)(spark)) {
        return
      } else if (FSUtils.directoryExists(sourcePath)(spark) && !FSUtils.directoryExists(destPath)(spark)) {
        val graph = LightCrossDeviceGraphDataset().readPartition(date, lookBack=Some(6))(spark)
          .withColumnRenamed("uiid", "TDID")
          .select("personID", "TDID", "deviceType")
          .distinct

        val sampledGraph = downSampleGraph(graph, shouldConsiderTDID2)

        sampledGraph.write.parquet(destPath)

        return
      }
    }
  }
}
