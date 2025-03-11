package com.thetradedesk.audience.jobs

import com.thetradedesk.audience.configs.AudienceModelInputGeneratorConfig
import com.thetradedesk.audience.datasets._
import com.thetradedesk.audience.{dateTime, shouldConsiderTDID2}
import com.thetradedesk.audience.jobs.DeviceBrowserMembershipGenerator.Config
import com.thetradedesk.audience.{date, dateTime, _}
import com.thetradedesk.geronimo.bidsimpression.schema.{BidsImpressions, BidsImpressionsSchema}
import com.thetradedesk.geronimo.shared.{GERONIMO_DATA_SOURCE, loadParquetData, readModelFeatures}
import com.thetradedesk.spark.TTDSparkContext.spark
import com.thetradedesk.spark.util.prometheus.PrometheusClient
import org.apache.spark.sql.{Column, DataFrame, SaveMode}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import com.thetradedesk.spark.TTDSparkContext.spark.implicits._
import com.thetradedesk.audience.jobs.modelinput.rsmv2.usersampling.SIBSampler.isDeviceIdSampled
import com.thetradedesk.geronimo.shared.transform.ModelFeatureTransform
import com.thetradedesk.spark.sql.SQLFunctions.{DataSetExtensions, SymbolExtensions}
import com.thetradedesk.spark.util.TTDConfig.{config, defaultCloudProvider}

import java.time.LocalDate
import java.time.format.DateTimeFormatter

object DensityInferenceDataGenerator {
  //val DensityScoreS3Path = "s3://thetradedesk-mlplatform-us-east-1/users/yixuan.zheng/trm_play/albertson/1105/user_zip_site/"
  val DensityScoreS3Path = config.getString("DensityInferenceDataGenerator.DensityScoreS3Path", "s3://thetradedesk-mlplatform-us-east-1/features/feature_store/prod/profiles/source=bidsimpression/index=TDID/config=TDIDDensityScoreSplit/v=1/date=20250208/")
  val prometheus = new PrometheusClient("DiagnosisJob", "DiagnosisDataMonitor")
  val diagnosisMetricsCount = prometheus.createGauge("distributed_algo_diagnosis_metrics_count", "Hourly counts of metrics from distributed algo diagnosis pipeline", "roigoaltype", "type")

  val sampleFactor = config.getInt("DensityInferenceDataGenerator.SampleFactor", default = 1)
  val modelVersion = s"${dateTime.format(DateTimeFormatter.ofPattern(audienceVersionDateFormat))}"
  val featuresJsonPath = s"s3://thetradedesk-mlplatform-us-east-1/models/prod/RSM/full_model/${modelVersion}/features.json"
  val TdidFreqCap = config.getInt("DensityInferenceDataGenerator.TdidFreqCap", default = 3)

  def runETLPipeline(): Unit = {
    val seeds = spark.read.format("parquet").load("s3://thetradedesk-mlplatform-us-east-1/data/prod/audience/relevanceseedsdataset/v=1/date=20250124/").collect()
    val syntheticIds: Array[Int] = seeds(0).getSeq(1).toArray[Int]
    val syntheticIdsSensi: Array[Int] = seeds(0).getSeq(3).toArray[Int]

    val bidImpressionsS3Path = BidsImpressions.BIDSIMPRESSIONSS3 + "prod/bidsimpressions/"
    var bidsImpressions = loadParquetData[BidsImpressionsSchema](bidImpressionsS3Path, date, source = Some(GERONIMO_DATA_SOURCE), lookBack=Some(0))
      .withColumn("TDID", getUiid('UIID, 'UnifiedId2, 'EUID, 'IdType))
      .filter('TDID.isNotNullOrEmpty && 'TDID =!= lit("00000000-0000-0000-0000-000000000000") && length('TDID) === 36)
      .filter(isDeviceIdSampled('TDID))
      .select(
        'AliasedSupplyPublisherId,
        'Site,
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
        'sin_hour_day,
        'cos_hour_day,
        'sin_hour_week, // time based features sometime are useful than expected
        'cos_hour_week,
        'sin_minute_hour,
        'cos_minute_hour,
        'Latitude,
        'Longitude,
        'InternetConnectionType,
        'OperatingSystem,


        'BidRequestId, // use to connect with bidrequest, to get more features
        'AdvertiserId,
        'AdGroupId,
        'SupplyVendor,
        'DealId,
        'SupplyVendorPublisherId,
        'AdWidthInPixels,
        'AdHeightInPixels,
        'MatchedFoldPosition,
        'sin_minute_day,
        'cos_minute_day,
        'CampaignId,
        'TDID,
        'LogEntryTime,
        'ContextualCategories,
        'MatchedSegments
      )
      // they saved in struct type
      .withColumn("OperatingSystemFamily", 'OperatingSystemFamily("value"))
      .withColumn("Browser", 'Browser("value"))
      .withColumn("RenderingContext", 'RenderingContext("value"))
      .withColumn("InternetConnectionType", 'InternetConnectionType("value"))
      .withColumn("OperatingSystem", 'OperatingSystem("value"))
      .withColumn("DeviceType", 'DeviceType("value"))
      .withColumn("Latitude", ('Latitude + lit(90.0)) / lit(180.0)) // -90 - 90
      .withColumn("Latitude", when('Latitude.isNotNull, 'Latitude).otherwise(0))
      .withColumn("Longitude", ('Longitude + lit(180.0)) / lit(360.0)) //-180 - 180
      .withColumn("Longitude", when('Longitude.isNotNull, 'Longitude).otherwise(0))
      .withColumn("SyntheticIds", typedLit(Seq.empty[Int]))
      .withColumn("ZipSiteLevel_Seed", typedLit(Seq.empty[Int]))
      .withColumn("MatchedSegmentsLength", when('MatchedSegments.isNull,0).otherwise(size('MatchedSegments)).cast(FloatType))
      .withColumn("HasMatchedSegments", when('MatchedSegmentsLength > lit(0), 1).otherwise(0))
      .withColumn("GroupId", 'TDID) // TODO:
      .withColumn("ContextualCategoriesTier1", typedLit(Array.empty[Int]))
      .withColumn("UserSegmentCount", lit(0.0))
      .withColumn("Targets", typedLit(Array.empty[Float]))
      .withColumn("SupplyVendor", lit(0))
      .withColumn("SupplyVendorPublisherId", lit(0))
      .withColumn("AdvertiserId", lit(0))

    if (sampleFactor > 1) {
      bidsImpressions = bidsImpressions.filter(abs(xxhash64('TDID)) % sampleFactor === lit(0))
    }

    val tdid2cnt = bidsImpressions.select('TDID).groupBy('TDID).agg(count('TDID).alias("TDIDFreq"))
    bidsImpressions = bidsImpressions.join(tdid2cnt, Seq("TDID"), "left")
      .withColumn("rand", rand())
      .filter(('TDIDFreq <= lit(TdidFreqCap))  or  ('TDIDFreq > lit(TdidFreqCap) and 'rand > ('TDIDFreq - lit(TdidFreqCap))*1.0/'TDIDFreq))
      .drop("rand", "TDIDFreq")

    val policyTable = AudienceModelPolicyReadableDataset(AudienceModelInputGeneratorConfig.model)
      .readSinglePartition(dateTime)(spark)
      .filter((col("CrossDeviceVendorId") === 0) && (col("IsActive") === true))
      .select("SourceId", "SyntheticId")

    // select only the online available seed
    val campaignSeed = CampaignSeedDataset().readPartition(date)
      .join(
        broadcast(policyTable),
        col("SeedId") === col("SourceId"),
        "inner"
      ).repartition(AudienceModelInputGeneratorConfig.bidImpressionRepartitionNumAfterFilter, 'CampaignId)

    // an campaign can have multiple seeds
    val campaignSeeds = campaignSeed.groupBy('CampaignId).agg(collect_list('SyntheticId).alias("SyntheticIds"))

    //val joinedImpressionsWithCampaignSeed = bidsImpressions
    //  .join(broadcast(campaignSeeds), Seq("CampaignId"), "inner")
      //.withColumn("SyntheticIdsTmp", $"SyntheticId".cast(IntegerType))


    val densityScore = spark.read.format("parquet").load(DensityScoreS3Path).select('TDID, 'SyntheticId_Level1, 'SyntheticId_Level2)

//    // load the density feature
//    val densityScore = spark.read.format("parquet").load(DensityScoreS3Path).select('TDID, 'score)
//      .withColumn("ZipSiteLevel_Seed",
//        when(col("score") < 0.8, 1)
//          .when((col("score") >= 0.8) && (col("score") < 0.99), 2)
//          .when(col("score") >= 0.99, 3)
//          .otherwise(0)
//      )
//      .drop("score")

    // join impression with density feature

    //val impWithDensity = joinedImpressionsWithCampaignSeed.join(densityScore, Seq("TDID"), "left")
    //  .withColumn("ZipSiteLevel_Seed", densityFeatureBySyntheticaId(col("SyntheticIds"), lit(null), col("SyntheticId_Level1"), col("SyntheticId_Level2"), lit(syntheticIdsSensi)))
//      .withColumn(
//        "ZipSiteLevel_Seed",
//        when(array_contains(col("SyntheticId_Level2"), col("SyntheticIdsTmp")), 2)
//          .when(array_contains(col("SyntheticId_Level1"), col("SyntheticIdsTmp")), 1)
//          .otherwise(0)
//      ).drop("SyntheticIdsTmp", "SyntheticId_Level1", "SyntheticId_Level2")

    //.selectAs[RelevanceOfflineRecord]

    val dataset = ModelFeatureTransform.modelFeatureTransform[AudienceModelV2InputRecord](bidsImpressions, readModelFeatures(featuresJsonPath))
    // join SyntheticIds
//    val joinedImpressionsWithCampaignSeed = dataset.drop("SyntheticIds", "ZipSiteLevel_Seed")
//      .join(broadcast(campaignSeeds), Seq("CampaignId"), "inner")

    // join DensityFeature
    val impWithDensity = dataset.join(densityScore, Seq("TDID"), "left")
      .withColumn("ZipSiteLevel_Seed", densityFeatureBySyntheticaId(lit(syntheticIds), lit(null), col("SyntheticId_Level1"), col("SyntheticId_Level2"), lit(syntheticIdsSensi)))
      .drop("SyntheticId_Level1", "SyntheticId_Level2")


    AudienceModelV2InputDataset(Model.RSMV2.toString,tag = "Density", version = 2)
      .writePartition(
        dataset = impWithDensity.as[AudienceModelV2InputRecord],
        partition = dateTime,
        format = Some("tfrecord"),
        saveMode = SaveMode.Overwrite,
        numPartitions = Some(30000)
      )



//    RelevanceOfflineDataset(config.getInt("version", default = 1))
//      .writePartition(
//        impWithDensity,
//        dateTime,
//        saveMode = SaveMode.Overwrite,
//        format = Some("tfrecord")
//      )
  }


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

  def main(args: Array[String]): Unit = {
    runETLPipeline()
    //prometheus.pushMetrics()
  }
}