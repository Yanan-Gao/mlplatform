package com.thetradedesk.audience.jobs

import com.thetradedesk.audience.configs.AudienceModelInputGeneratorConfig
import com.thetradedesk.audience.datasets._
import com.thetradedesk.audience.jobs.modelinput.rsmv2.usersampling.SIBSampler.{guidToLongs, isDeviceIdSampled, userIsSampled}
import com.thetradedesk.audience.{getUiid, _}
import com.thetradedesk.geronimo.bidsimpression.schema.{BidsImpressions, BidsImpressionsSchema}
import com.thetradedesk.geronimo.shared.transform.ModelFeatureTransform
import com.thetradedesk.geronimo.shared.{GERONIMO_DATA_SOURCE, loadParquetData, readModelFeatures}
import com.thetradedesk.spark.TTDSparkContext.spark
import com.thetradedesk.spark.TTDSparkContext.spark.implicits._
import com.thetradedesk.spark.sql.SQLFunctions.SymbolExtensions
import com.thetradedesk.spark.util.TTDConfig.config
import org.apache.spark.sql.catalyst.expressions.GenericRowWithSchema
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.{Row, SaveMode, SparkSession}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._

import java.nio.ByteBuffer
import java.security.MessageDigest
import java.util.UUID
//import com.adbrain.deviceatlas.DeviceAtlasUserAgentParser
import org.apache.spark.sql.catalyst.encoders.RowEncoder

import java.time.format.DateTimeFormatter
import scala.util.Random

object AvailDensityInferenceDataGenerator {

  val sampleUDF = shouldConsiderTDID3(config.getInt("onlineBiddingDownSampleRate", default = 1000000), config.getString("saltToSampleHitRate", default = "RelevanceOnline"))(_)

  val PublisherIdMapS3 = config.getString("AvailDensityInferenceDataGenerator.PublisherIdMapS3", "s3://thetradedesk-useast-qubole/warehouse.external/thetradedesk.db/provisioning/supplyvendorpublishersellerv2/v=1/date=20250210/")
  val AvailS3Path = config.getString("AvailDensityInferenceDataGenerator.AvailS3Path", "s3://ttd-identity/datapipeline/prod/avails7day/v=2/date=20250206/")
  val DensityScoreS3Path = config.getString("AvailDensityInferenceDataGenerator.DensityScoreS3Path", "s3://thetradedesk-mlplatform-us-east-1/features/feature_store/prod/profiles/source=bidsimpression/index=TDID/config=TDIDDensityScoreSplit/v=1/date=20250205/")
  val CityS3Path = config.getString("AvailDensityInferenceDataGenerator.CityS3Path", "s3://thetradedesk-useast-qubole/warehouse.external/thetradedesk.db/provisioning/city/v=1/date=20250209/")
  //val prometheus = new PrometheusClient("DiagnosisJob", "DiagnosisDataMonitor")
  //val diagnosisMetricsCount = prometheus.createGauge("distributed_algo_diagnosis_metrics_count", "Hourly counts of metrics from distributed algo diagnosis pipeline", "roigoaltype", "type")

  val sampleFactor = config.getInt("AvailDensityInferenceDataGenerator.SampleFactor", default = 1)
  val TdidFreqCap = config.getInt("AvailDensityInferenceDataGenerator.TdidFreqCap", default = 3)
  val modelVersion = s"${dateTime.format(DateTimeFormatter.ofPattern(audienceVersionDateFormat))}"
  val featuresJsonPath = config.getString("AvailDensityInferenceDataGenerator.FeatureS3", s"s3://thetradedesk-mlplatform-us-east-1/models/prod/RSM/full_model/${modelVersion}/features.json")

  val TIME_ROUNDING_PRECISION = 6
  // used for time of day features.  Pi is hardcoded here because the math.pi values differ between java/scala
  // and c# where the time of day features are computed at bid time.
  val TWOPI = 2 * 3.14159265359

  val zeroId = "00000000-0000-0000-0000-000000000000"



  val translateIdType = udf((deviceId: String, tdid: String) => {
    if (deviceId == null || deviceId.isEmpty) {
      if (tdid == null || tdid.isEmpty) "missingId" else if (tdid == zeroId) "optOutTdid" else "validTdid"
    } else if (deviceId == zeroId) {
      if (tdid == null || tdid.isEmpty || tdid == zeroId) "optOutDvid" else "optOutDvidWithValidTdid"
    } else {
      if (tdid == null || tdid.isEmpty) "validDvid" else if (tdid == zeroId) "validDvidWithOptOutTdid" else "validBoth"
    }
  })


//  def runETLPipeline(): Unit = {
//    // STEP 1: Load Avail Data
//    var availsOri = spark.read.format("parquet").load(AvailS3Path)
//      .withColumn("TDID", coalesce($"TdidGuid",$"DeviceIdGuid",$"UnifiedId2")) // TODO: is this the right logic?
//      // copied from https://gitlab.adsrvr.org/thetradedesk/teams/idnt/etl/-/blob/master/src/main/scala/com/thetradedesk/etl/identityalliance/AvailsTrafficMonitor.scala#L178
//      .withColumn("idType", translateIdType($"DeviceIdGuid", $"TdidGuid"))
//      .filter("TDID is not NULL and TDID <> '00000000-0000-0000-0000-000000000000'")
//      .filter(isDeviceIdSampled('TDID))
//
//    if (sampleFactor > 1) {
//      availsOri = availsOri.filter(abs(xxhash64('TDID)) % sampleFactor === lit(0))
//    }
//
//    val encoder = RowEncoder.encoderFor(availsOri.schema.add("Browser", StringType)
//      .add("OperatingSystem", StringType)
//      .add("DeviceType", StringType))
//    val availsExt = availsOri.mapPartitions(parseUserAgent(spark))(encoder)
//    val avails =  availsExt.drop("UserAgent")
//      .withColumn("LogEntryTime",from_unixtime($"TimeStamp" / 1000).cast("timestamp"))
//      .withColumn("sin_hour_day", round(sin(lit(TWOPI) * hour(col("LogEntryTime")) / 24), TIME_ROUNDING_PRECISION))
//      .withColumn("cos_hour_day", round(cos(lit(TWOPI) * hour(col("LogEntryTime")) / 24), TIME_ROUNDING_PRECISION))
//      // hour in the week, need to subtract day of week by 1 because days are 1-7
//      .withColumn("sin_hour_week", round(sin(lit(TWOPI) * (hour(col("LogEntryTime")) + ((dayofweek(col("LogEntryTime")) - 1) * 24)) / (7 * 24)), TIME_ROUNDING_PRECISION))
//      .withColumn("cos_hour_week" , round(cos(lit(TWOPI) * (hour(col("LogEntryTime")) + ((dayofweek(col("LogEntryTime")) - 1) * 24)) / (7 * 24)), TIME_ROUNDING_PRECISION))
//      // minute in the hour
//      .withColumn("sin_minute_hour", round(sin(lit(TWOPI) * minute(col("LogEntryTime")) / 60), TIME_ROUNDING_PRECISION))
//      .withColumn("cos_minute_hour" , round(cos(lit(TWOPI) * minute(col("LogEntryTime")) / 60), TIME_ROUNDING_PRECISION))
//      // minute in the week
//      .withColumn("sin_minute_day", round(sin(lit(TWOPI) * (minute(col("LogEntryTime")) + (hour(col("LogEntryTime")) * 60)) / (24 * 60)), TIME_ROUNDING_PRECISION))
//      .withColumn("cos_minute_day", round(cos(lit(TWOPI) * (minute(col("LogEntryTime")) + (hour(col("LogEntryTime")) * 60)) / (24 * 60)), TIME_ROUNDING_PRECISION))
//      // .withColumn("AdWidthInPixels", ('AdWidthInPixels - lit(1.0)) / lit(9999.0)) // 1 - 10000
//      // .withColumn("AdWidthInPixels", when('AdWidthInPixels.isNotNull, 'AdWidthInPixels).otherwise(0))
//      // .withColumn("AdHeightInPixels", ('AdHeightInPixels - lit(1.0)) / lit(9999.0)) // 1 - 10000
//      // .withColumn("AdHeightInPixels", when('AdHeightInPixels.isNotNull, 'AdHeightInPixels).otherwise(0))
//      .withColumn("Latitude", ('Latitude + lit(90.0)) / lit(180.0)) // -90 - 90
//      .withColumn("Latitude", when('Latitude.isNotNull, 'Latitude).otherwise(0))
//      .withColumn("Longitude", ('Longitude + lit(180.0)) / lit(360.0)) //-180 - 180
//      .withColumn("Longitude", when('Longitude.isNotNull, 'Longitude).otherwise(0))
//      .withColumn("DealId", explode(col("DealIds")))
//      .withColumn("Site", when(size('Urls) > 0, call_udf("parse_url", $"Urls"(0), lit("HOST"))).otherwise(null))
//      .withColumn("BidRequestId", lit(""))
//      .withColumn("AdGroupId", lit(""))
//      .withColumn("BidRequestId", lit(""))
//      .withColumn("GroupId", 'TDID) // TODO:
//      .withColumn("ContextualCategoriesTier1", typedLit(Array.empty[Int]))
//      .withColumn("UserSegmentCount", lit(0.0))
//      .withColumn("Targets", typedLit(Array.empty[Float]))
//      .withColumn("SupplyVendor", lit(0))
//      .withColumn("SupplyVendorPublisherId", lit(0))
//      .withColumn("AdWidthInPixels", lit(0))
//      .withColumn("AdHeightInPixels", lit(0))
//      .withColumn("MatchedSegmentsLength", lit(0).cast(FloatType))
//      .withColumn("HasMatchedSegments", lit(0).cast(IntegerType))
//      .withColumn("MatchedSegments", typedLit(Array.empty[Long]))
//      .withColumn("OperatingSystemFamily", lit(""))
//      //.withColumn("Browser", lit("")) // Generated by parseUserAgent
//      .withColumn("RenderingContext", lit(""))
//      .withColumn("InternetConnectionType", lit(""))
//      //.withColumn("OperatingSystem", lit("")) // Generated by parseUserAgent
//      //.withColumn("DeviceType", lit("")) // Generated by parseUserAgent
//      .withColumn("MatchedFoldPosition", lit(0))
//      .withColumn("RequestLanguages", lit(0))
//      .withColumn("City", lit(0))
//      .withColumn("AliasedSupplyPublisherId", lit(0))
//
//
//    // TODO: Extract DeviceModel https://gitlab.adsrvr.org/thetradedesk/teams/idnt/etl/-/blob/master/src/main/scala/com/thetradedesk/etl/identityalliance/AvailsTrafficMonitor.scala
//    // TODO: Extract DeviceMake
//    // ...
//
//
//    // Step 2: Load BidRequest and Campaign
//    val bidRequestCache = spark.read.format("parquet").load(BigRequestS3Path)
//      .select("CampaignId",  "DealId", "AdvertiserId", "AdGroupId")
//      .cache()
//
//    val adGroupIds = bidRequestCache
//        .groupBy("CampaignId")
//        .agg(collect_set("AdGroupId").alias("AdGroupIds"))
//
//    val bidRequest = bidRequestCache.select("CampaignId",  "DealId", "AdvertiserId").distinct()
//      .join(adGroupIds, Seq("CampaignId"), "left")
//
//      //.withColumn("AdGroupIds", collect_set("AdGroupId").over(Window.partitionBy("CampaignId")).alias("AdGroupIds"))
//      //.groupBy("CampaignId",  "DealId", "AdvertiserId")
//      //.agg(any_value($"AdGroupIds"))
//
//
//    val policyTable = AudienceModelPolicyReadableDataset(AudienceModelInputGeneratorConfig.model)
//      .readSinglePartition(dateTime)(spark)
//      //.filter((col("CrossDeviceVendorId") === 0) && (col("IsActive") === true))
//      .filter("CrossDeviceVendorId = 0 and IsActive")
//      .select("SourceId", "SyntheticId", "ActiveSize")
//
//    // select only the online available seed
//    val campaign2Seeds = CampaignSeedDataset().readPartition(date)
//      .join(
//        broadcast(policyTable),
//        col("SeedId") === col("SourceId"),
//        "inner"
//      ).repartition(AudienceModelInputGeneratorConfig.bidImpressionRepartitionNumAfterFilter, 'CampaignId)
//      .groupBy("CampaignId")
//      .agg(collect_set("SyntheticId").alias("SyntheticIds"))
//
////    val campaign2Seeds = CampaignSeedDataset().readPartition(date)
////      .select("CampaignId", "SyntheticId")
////      .withColumn("SyntheticId", $"SyntheticId".cast(IntegerType))
////      .groupBy("CampaignId")
////      .agg(collect_set("SyntheticId"))
//
//    // Step 3: Join Avails with BidRequest and Campaign to get the SyntheticIds and AdGroupId
//    val availJoined = avails
//      .join(broadcast(bidRequest), Seq("DealId"), "left")
//      .join(broadcast(campaign2Seeds), Seq("CampaignId"), "left")
//      //.withColumn("SyntheticIdsTmp", $"SyntheticId".cast(IntegerType))
//
//    // Step 4: Load Density Feature
//    val densityScore = spark.read.format("parquet").load(DensityScoreS3Path).select('TDID, 'SyntheticId_Level1, 'SyntheticId_Level2)
//
//    // Step 5: Join Density Feature
//    val result = availJoined.join(densityScore, Seq("TDID"), "left")
//      .withColumn(
//        "ZipSiteLevel_Seed",
//        when(arrays_overlap(col("SyntheticId_Level2"), col("SyntheticIds")), 2)
//          .when(arrays_overlap(col("SyntheticId_Level1"), col("SyntheticIds")), 1)
//          .otherwise(0)
//      ).drop("SyntheticId_Level1", "SyntheticId_Level2")
//
//
//    val availsFeat = ModelFeatureTransform.modelFeatureTransform[AudienceModelV2InputRecord](result, readModelFeatures(featuresJsonPath))
//
//
//    AudienceModelV2InputDataset(Model.RSMV2.toString,tag = "Avail", version = 2)
//      .writePartition(
//        dataset = result.as[AudienceModelV2InputRecord],
//        partition = dateTime,
//        format = Some("tfrecord"),
//        saveMode = SaveMode.Overwrite
//      )
//  }

  val deviceTypeMap = Map(
    "d" -> "desktop",
    "t" -> "tablet",
    "v" -> "TV",
    "m" -> "mobile",
    "n" -> "unknown",
    "g" -> "game_console"
  )

  def translateDeviceTypeEnum(deviceType: String): String = {
    if (deviceType == null || deviceType.isEmpty)
      "unknown"
    else {
      deviceTypeMap.getOrElse(deviceType, "unknown")
    }
  }


  def convertUID2ToGuid()(rawIterator: Iterator[Row]) : Iterator[Row] = {
    val md5 = MessageDigest.getInstance("MD5") // assume this is
    new Iterator[Row] {
      override def hasNext : Boolean = rawIterator.hasNext
      var Uid2Idx: Int = -1;
      var UnifiedId2GuidIdx: Int = -1;
      override def next() : Row = {
        val rawRow = rawIterator.next()
        if (Uid2Idx < 0) {
          Uid2Idx = rawRow.fieldIndex("UnifiedId2")
          UnifiedId2GuidIdx = rawRow.fieldIndex("UnifiedId2Guid")
        }
        val rawUid2 = rawRow.getString(Uid2Idx)
        if (rawUid2 == null) {
          return rawRow
        }
        val hashBytes = md5.digest(rawUid2.getBytes("ASCII"))

        val bb = ByteBuffer.wrap(hashBytes)
        val high = bb.getLong
        val low = bb.getLong

        val uuid = new UUID(high, low)
        val newRow = rawRow.toSeq.toArray.updated(UnifiedId2GuidIdx, uuid.toString)
        new GenericRowWithSchema(newRow, rawRow.schema)
      }

    }
  }
//  def parseUserAgent(spark: SparkSession)(rawIterator: Iterator[Row]) : Iterator[Row] = {
//    val userAgentParser = new DeviceAtlasUserAgentParser(5000)(spark)
//    new Iterator[Row] {
//
//      override def hasNext : Boolean = rawIterator.hasNext
//
//      var userAgentIdx: Int = -1;
//
//      override def next() : Row = {
//        val rawRow = rawIterator.next()
//        if (userAgentIdx < 0) {
//          userAgentIdx = rawRow.fieldIndex("UserAgent")
//        }
//        val userAgent = rawRow.getString(userAgentIdx)
//
//        val (browser, os, osVersion, deviceType) = if (userAgent == null || userAgent.isEmpty) {
//          ("unknown", "unknown", "", "unknown")
//        }
//        else {
//          val uaProperties = userAgentParser.parse(userAgent)
//          val browser = if (uaProperties.browser.contains("InternetExplorer")) "InternetExplorer" else uaProperties.browser
//          val deviceType = translateDeviceTypeEnum(uaProperties.deviceType)
//          val os = if (uaProperties.OS.isEmpty) "unknown" else uaProperties.OS
//          (browser, os, uaProperties.osVersion, deviceType)
//        }
//
//        //new GenericRowWithSchema((rawRow.toSeq :+ "test val").toArray, rawRow.schema.add("testCol", StringType))
//        Row.fromSeq(rawRow.toSeq ++ Seq(os))
//      }
//    }
//  }

//  private val seeds = spark.read.format("parquet").load("s3://thetradedesk-mlplatform-us-east-1/data/prod/audience/relevanceseedsdataset/v=1/date=20250124/").collect()
//  val syntheticIds: Array[Int] = seeds(0).getSeq(1).toArray[Int]
//  val syntheticIdsCount:Int = syntheticIds.length


  // 1, update the logic to get tdid, use getUiid
  // 2, confirm the RequestLanguages logic
  // 3, No Targets for inference data
  // 4, Generate ZipSite_Level to array, corresponding to SyntheticalIds
  //    4.1 join with TDID first
  //    4.2 if TDID does not join, then join by abs(hash(zip || site || TRM)) with another dataset
  //    4.3 if none joined, then assign it to 0 if it's sensitive, otherwise 1
  def runETLPipelineRandomAssign(): Unit = {

    val aggSeed = AggregatedSeedReadableDataset()
      .readPartition(date)(spark)
      .select("TDID","SeedIds")
      .where(sampleUDF(Symbol(config.getString("hashSampleObject", default = "TDID")))) // use this to control the filtering object, if error, then the object is not in aggregatedseed table
      .repartition(AudienceModelInputGeneratorConfig.bidImpressionRepartitionNumAfterFilter, 'TDID)


    val seeds = spark.read.format("parquet").load("s3://thetradedesk-mlplatform-us-east-1/data/prod/audience/relevanceseedsdataset/v=1/date=20250124/").collect()
    val syntheticIds: Array[Int] = seeds(0).getSeq(1).toArray[Int]
    val syntheticIdsSensi: Array[Int] = seeds(0).getSeq(3).toArray[Int]


    // STEP 1: Load Avail Data
    val availsOriUid2 = spark.read.format("parquet").load(AvailS3Path)
      .withColumn("UnifiedId2Guid", lit(""))

      var availsOri = availsOriUid2
        .mapPartitions(convertUID2ToGuid())(availsOriUid2.encoder)
      .withColumn("TDID", coalesce($"TdidGuid",$"DeviceIdGuid",$"UnifiedId2Guid")) // TODO: is this the right logic?
      // .withColumn("TDID", getUiid('UIID, 'UnifiedId2, 'EUID, 'IdType)) // Note, avail has not Uiid
      // copied from https://gitlab.adsrvr.org/thetradedesk/teams/idnt/etl/-/blob/master/src/main/scala/com/thetradedesk/etl/identityalliance/AvailsTrafficMonitor.scala#L178
      .withColumn("idType", translateIdType($"DeviceIdGuid", $"TdidGuid"))
      .filter("TDID is not NULL and TDID <> '00000000-0000-0000-0000-000000000000'")
      .filter(isDeviceIdSampled('TDID))

    if (sampleFactor > 1) {
      availsOri = availsOri.filter(abs(xxhash64('TDID)) % sampleFactor === lit(0))
    }

    // we only keep 10 avails per TDID
    val tdid2cnt = availsOri.select("TDID").groupBy("TDID").agg(count("TDID").alias("TDIDFreq"))
    availsOri = availsOri.join(tdid2cnt, Seq("TDID"), "left")
      .withColumn("rand", rand())
      .filter(('TDIDFreq <= lit(TdidFreqCap))  or  ('TDIDFreq > lit(TdidFreqCap) and 'rand > ('TDIDFreq - lit(TdidFreqCap))*1.0/'TDIDFreq))
      .drop("rand", "TDIDFreq")



    //    val encoder = RowEncoder(
//      availsOri.schema
//        //.add("Browser", StringType)
//      .add("OperatingSystem", StringType)
//      //.add("DeviceType", StringType)
//    )
    //val availsExt = availsOri.mapPartitions(parseUserAgent(spark))(encoder)
    val avails =  availsOri.drop("UserAgent")
      .withColumn("LogEntryTime",from_unixtime($"TimeStamp" / 1000).cast("timestamp"))
      .withColumn("sin_hour_day", round(sin(lit(TWOPI) * hour(col("LogEntryTime")) / 24), TIME_ROUNDING_PRECISION))
      .withColumn("cos_hour_day", round(cos(lit(TWOPI) * hour(col("LogEntryTime")) / 24), TIME_ROUNDING_PRECISION))
      // hour in the week, need to subtract day of week by 1 because days are 1-7
      .withColumn("sin_hour_week", round(sin(lit(TWOPI) * (hour(col("LogEntryTime")) + ((dayofweek(col("LogEntryTime")) - 1) * 24)) / (7 * 24)), TIME_ROUNDING_PRECISION))
      .withColumn("cos_hour_week" , round(cos(lit(TWOPI) * (hour(col("LogEntryTime")) + ((dayofweek(col("LogEntryTime")) - 1) * 24)) / (7 * 24)), TIME_ROUNDING_PRECISION))
      // minute in the hour
      .withColumn("sin_minute_hour", round(sin(lit(TWOPI) * minute(col("LogEntryTime")) / 60), TIME_ROUNDING_PRECISION))
      .withColumn("cos_minute_hour" , round(cos(lit(TWOPI) * minute(col("LogEntryTime")) / 60), TIME_ROUNDING_PRECISION))
      // minute in the week
      .withColumn("sin_minute_day", round(sin(lit(TWOPI) * (minute(col("LogEntryTime")) + (hour(col("LogEntryTime")) * 60)) / (24 * 60)), TIME_ROUNDING_PRECISION))
      .withColumn("cos_minute_day", round(cos(lit(TWOPI) * (minute(col("LogEntryTime")) + (hour(col("LogEntryTime")) * 60)) / (24 * 60)), TIME_ROUNDING_PRECISION))
      // .withColumn("AdWidthInPixels", ('AdWidthInPixels - lit(1.0)) / lit(9999.0)) // 1 - 10000
      // .withColumn("AdWidthInPixels", when('AdWidthInPixels.isNotNull, 'AdWidthInPixels).otherwise(0))
      // .withColumn("AdHeightInPixels", ('AdHeightInPixels - lit(1.0)) / lit(9999.0)) // 1 - 10000
      // .withColumn("AdHeightInPixels", when('AdHeightInPixels.isNotNull, 'AdHeightInPixels).otherwise(0))
      //.withColumn("Latitude", ('Latitude + lit(90.0)) / lit(180.0)) // -90 - 90
      .withColumn("Latitude", 'Latitude / lit(10000000.0))
      .withColumn("Latitude", when('Latitude.isNotNull, 'Latitude).otherwise(0))
      .withColumn("Latitude", ('Latitude + lit(90.0)) / lit(180.0)) // -90 - 90
      .withColumn("Longitude", 'Longitude / lit(10000000.0))
      //.withColumn("Longitude", ('Longitude + lit(180.0)) / lit(360.0)) //-180 - 180
      .withColumn("Longitude", when('Longitude.isNotNull, 'Longitude).otherwise(0))
      .withColumn("Longitude", ('Longitude + lit(180.0)) / lit(360.0)) //-180 - 180
      //.withColumn("DealId", explode(col("DealIds")))
      .withColumn("Site", when(size('Urls) > 0, call_udf("parse_url", $"Urls"(0), lit("HOST"))).otherwise(null))
      .withColumn("BidRequestId", lit(""))// Ignore
      .withColumn("AdGroupId", lit(""))
      .withColumn("BidRequestId", lit(""))
      .withColumn("GroupId", 'TDID) // TODO:
      .withColumn("ContextualCategoriesTier1", typedLit(Array.empty[Int]))
      .withColumn("UserSegmentCount", lit(0.0))
      .withColumn("Targets", typedLit(Array.empty[Float])) // Low
      .withColumn("SupplyVendor", lit(0))
      //.withColumn("SupplyVendorPublisherId", lit(0)) // be joined later
      .withColumn("AdWidthInPixels", lit(0))
      .withColumn("AdHeightInPixels", lit(0))
      .withColumn("MatchedSegmentsLength", lit(0).cast(FloatType)) // ignore
      .withColumn("HasMatchedSegments", lit(0).cast(IntegerType)) // ignore
      .withColumn("MatchedSegments", typedLit(Array.empty[Long])) // ignore
      .withColumn("OperatingSystemFamily", lit(0)) // ignore
      .withColumn("OperatingSystem", lit(0)) // ignore
      //.withColumn("RenderingContext", lit(""))
      .withColumn("InternetConnectionType", lit(0)) // Not found, Ignore
      .withColumn("OperatingSystem", lit("")) // Not found, can be Generated by parseUserAgent

      .withColumn("MatchedFoldPosition", lit(0))
      .withColumn("RequestLanguages", expr("concat_ws(',', LanguageCodes)"))
      //.withColumn("City", lit(0)) // Not found
      //.withColumn("AliasedSupplyPublisherId", col("SupplyVendorPublisherId"))
      //.withColumn("SyntheticIds", randomIds(lit(10), lit(syntheticIds)))
      .withColumn("SyntheticIds", typedLit(Array.empty[Int]))
      .withColumn("CampaignId", lit(0))
      .withColumn("AdvertiserId", lit(0))


    // Step 3: map to
    val publiserMap = spark.read.format("parquet").load(PublisherIdMapS3)
      .select(col("SupplyVendorPublisherId"), col("PublisherId"))
      // one SupplyVendorPublisherId has ~10 AliasedSupplyPublisherId, so here we just keep one
      .groupBy("SupplyVendorPublisherId").agg(first("PublisherId").alias("AliasedSupplyPublisherId"))


    val availsAliasedSv = avails.join(broadcast(publiserMap), Seq("SupplyVendorPublisherId"))
      .drop("SupplyVendorPublisherId")
      .withColumn("SupplyVendorPublisherId", lit(0))

    // Step 4: Load Density Feature
    val densityScore = spark.read.format("parquet").load(DensityScoreS3Path).select('TDID, 'SyntheticId_Level1, 'SyntheticId_Level2)

    // Step 5: join city
    val dfCity = spark.read.format("parquet").load(CityS3Path).select(col("Name").alias("City"), col("MaxMindLocationId").alias("CityMaxMindLocationId"))
    val withCity = availsAliasedSv.join(broadcast(dfCity), Seq("CityMaxMindLocationId"), "left")

    // Step 6: Generate ZipSiteLevel_Seed array
    val resultWithDensity = withCity.join(densityScore, Seq("TDID"), "left")
      .withColumn("ZipSiteLevel_Seed", densityFeatureBySyntheticaId(lit(syntheticIds), lit(null), col("SyntheticId_Level1"), col("SyntheticId_Level2"), lit(syntheticIdsSensi)))
      .drop("SyntheticId_Level1", "SyntheticId_Level2")

    val result = resultWithDensity.join(aggSeed, Seq("TDID"), "left")
      .withColumn("Targets", targetsBySyntheticaId(col("SeedIds"), lit(syntheticIds)))

    // Step 7: TODO: join with density feature by zip and site hash, to fill those null ZipSiteLevel_Seed
    // pending on Nikunji, to provide that dataset


    val availsFeat = ModelFeatureTransform.modelFeatureTransform[AudienceModelV2InputRecord](result, readModelFeatures(featuresJsonPath))


    AudienceModelV2InputDataset(Model.RSMV2.toString,tag = "Avail", version = 2)
      .writePartition(
        dataset = availsFeat.as[AudienceModelV2InputRecord],
        partition = dateTime,
        format = Some("tfrecord"),
        saveMode = SaveMode.Overwrite
      )
  }

  def randomIds_(num: Int, ids:Array[Int]): Array[Int] = {
    val size = if (num < ids.length) num else ids.length
    val ret = new Array[Int](size)
    for (i <- 0 until size)
      ret(i) = ids(Random.nextInt(ids.length))

    return ret
  }

  val randomIds = udf(randomIds_ _)

  def targetsBySyntheticaId_(seedIdsTdid:Array[String], seedsGlobal: Array[String]):Array[Float] = {
    if (seedIdsTdid == null) {
      return null
    }

    val seedSet = seedIdsTdid.toSet
    val size = seedsGlobal.length
    val ret = new Array[Float](size)
    for (i <- 0 until size) {
      ret(i) = if (seedSet.contains(seedsGlobal(i))) 1.0f else 0.0f
    }
    return ret
  }

  val targetsBySyntheticaId = udf(targetsBySyntheticaId_ _)

  def densityFeatureBySyntheticaId_(synthIds: Array[Int],
                                   existingDensity: Array[Int],
                                   SyntheticId_Level1: Array[Int],
                                   SyntheticId_Level2: Array[Int],
                                   sensitiveSyntheticaIdsArr: Array[Int]): Array[Int] = {
    if (SyntheticId_Level1 == null || SyntheticId_Level2 == null)
      return null

    if (existingDensity != null)
      return existingDensity

    val size = synthIds.length
    val ret = new Array[Int](size)
    val sensitiveSyntheticaIds = sensitiveSyntheticaIdsArr.toSet[Int]
    for (i <- 0 until size) {
      val id = synthIds(i)
      if (sensitiveSyntheticaIds.contains(id)) {
        ret(i) = 0
      } else if (SyntheticId_Level1.contains(id)) {
        ret(i) = 2
      } else if (SyntheticId_Level2.contains(id)) {
        ret(i) = 3
      } else {
        ret(i) = 1
      }
    }
    ret
  }

  val densityFeatureBySyntheticaId = udf(densityFeatureBySyntheticaId_ _)


  def convertUID2ToGUID(uid2: String) = {
    try {
      val md5 = MessageDigest.getInstance("MD5")
      val hashBytes = md5.digest(uid2.getBytes("ASCII"))

      val bb = ByteBuffer.wrap(hashBytes)
      val high = bb.getLong
      val low = bb.getLong

      val uuid = new UUID(high, low)
      uuid.toString
    } catch {
      case _: Exception => null
    }
  }

  val convertUID2ToGUIDUDF = udf(convertUID2ToGUID _)

  def main(args: Array[String]): Unit = {
    //spark.sparkContext.addFile("s3://ttd-identity/external/deviceatlas/2025-02-03/file.json.gz")
    //DeviceAtlasUserAgentParser.downloadDbAndDistributeToSparkWorkers("s3://ttd-identity/external/deviceatlas/2025-02-03/")(spark)
    runETLPipelineRandomAssign()
  }
}