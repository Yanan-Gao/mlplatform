package com.thetradedesk.audience.jobs

import com.thetradedesk.audience.configs.AudienceModelInputGeneratorConfig
import com.thetradedesk.audience.datasets._
import com.thetradedesk.audience.jobs.modelinput.rsmv2.RSMV2SharedFunction.{castDoublesToFloat, paddingColumns}
import com.thetradedesk.audience.jobs.modelinput.rsmv2.usersampling.SamplerFactory
import com.thetradedesk.audience.transform.ContextualTransform.generateContextualFeatureTier1
import com.thetradedesk.audience.transform.IDTransform.{filterOnIdTypesSym, idTypesBitmap, joinOnIdTypes}
import com.thetradedesk.audience.utils.IDTransformUtils.addGuidBits
import com.thetradedesk.audience.utils.SeedListUtils.activeCampaignSeedAndIdFilterUDF
import com.thetradedesk.audience._
import com.thetradedesk.confetti.AutoConfigResolvingETLJobBase
import com.thetradedesk.geronimo.bidsimpression.schema.{BidsImpressions, BidsImpressionsSchema}
import com.thetradedesk.geronimo.shared.transform.ModelFeatureTransform
import com.thetradedesk.geronimo.shared.{GERONIMO_DATA_SOURCE, loadParquetData, readModelFeatures}
import com.thetradedesk.spark.TTDSparkContext.spark
import com.thetradedesk.spark.TTDSparkContext.spark.implicits._
import com.thetradedesk.spark.util.prometheus.PrometheusClient
import org.apache.spark.sql.SaveMode
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._

import java.time.LocalDate

case class RelevanceOnlineBiddingDataGeneratorJobConfig(
  model: String,
  tag: String,
  subFolderKey: String,
  subFolderValue: String,
  sampleSalt: String,
  sampleRatio: Int,
  paddingColumns: Seq[String],
  runDate: LocalDate
)

object RelevanceOnlineBiddingDataGeneratorJob
  extends AutoConfigResolvingETLJobBase[RelevanceOnlineBiddingDataGeneratorJobConfig](
    groupName = "audience",
    jobName = "RelevanceOnlineBiddingDataGeneratorJob") {

  override val prometheus: Option[PrometheusClient] =
    Some(new PrometheusClient("ModelQualityJob", "RelevanceOnlineBiddingDataGeneratorJob"))

  override def runETLPipeline(): Unit = {
    val conf = getConfig
    new RelevanceOnlineBiddingDataGenerator(prometheus.get, confettiEnv, experimentName, conf).generate()
  }

  /**
   * for backward compatibility, local test usage.
   * */
  override def loadLegacyConfig(): RelevanceOnlineBiddingDataGeneratorJobConfig = ???
}


class RelevanceOnlineBiddingDataGenerator(prometheus: PrometheusClient,
                                          confettiEnv: String,
                                          experimentName: Option[String],
                                          conf: RelevanceOnlineBiddingDataGeneratorJobConfig) {

  val jobRunningTime = prometheus.createGauge(s"relevance_online_bidding_generation_job_running_time", "RelevanceOnlineBiddingDataGenerator running time", "date")
  val onlineRelevanceBiddingTableSize = prometheus.createGauge(s"online_relevance_bidding_table_size", "OnlineRelevanceBiddingTableGenerator table size", "date")
  val sampler = SamplerFactory.fromString("RSMV2Measurement", conf)

  def generate(): Unit = {
    val RSMV2UserSampleSalt = conf.sampleSalt
    val date = conf.runDate
    val dateTime = date.atStartOfDay()

    val start = System.currentTimeMillis()

    val (_, activeSeedIdFilterUDF) = activeCampaignSeedAndIdFilterUDF()

    val aggSeed = AggregatedSeedReadableDataset()
      .readPartition(date)(spark)
      .filter(col("IsOriginal").isNotNull)
      .withColumn("SeedIds", activeSeedIdFilterUDF('SeedIds))
      .withColumn("PersonGraphSeedIds", activeSeedIdFilterUDF('PersonGraphSeedIds))
      .withColumn("HouseholdGraphSeedIds", activeSeedIdFilterUDF('HouseholdGraphSeedIds))
      .repartition(AudienceModelInputGeneratorConfig.bidImpressionRepartitionNumAfterFilter, 'TDID)
      .cache()

    val bidImpressionsS3Path = BidsImpressions.BIDSIMPRESSIONSS3 + "prod/bidsimpressions/"
    val bidsImpressions = loadParquetData[BidsImpressionsSchema](bidImpressionsS3Path, date, lookBack = Some(0), source = Some(GERONIMO_DATA_SOURCE))
      .filter('IsImp)
      .filter(filterOnIdTypesSym(sampler.samplingFunction)) // featurestore should use the same split function to put all idtypes into the same bin
      .withColumn("TDID", getUiid('UIID, 'UnifiedId2, 'EUID, 'IdentityLinkId, 'IdType))
      .withColumn("TDID", when(col("TDID") === "00000000-0000-0000-0000-000000000000", null).otherwise(col("TDID")))
      .filter(col("TDID").isNotNull)
      .select('BidRequestId, // use to connect with bidrequest, to get more features
        'DeviceAdvertisingId,
        'CookieTDID,
        'UnifiedId2,
        'EUID,
        'IdentityLinkId,
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
        'LogEntryTime,
        'ContextualCategories,
        'MatchedSegments,
        'UserSegmentCount,
        'RelevanceScore,
        'TDID
      )
      .filter('RelevanceScore.isNotNull && 'RelevanceScore >= 0 && 'RelevanceScore <= 1)
      .withColumnRenamed("RelevanceScore", "OnlineRelevanceScore")
      .withColumn("OnlineRelevanceScore", col("OnlineRelevanceScore").cast("float"))
      // they saved in struct type
      .withColumn("OperatingSystemFamily", 'OperatingSystemFamily("value"))
      .withColumn("Browser", 'Browser("value"))
      .withColumn("RenderingContext", 'RenderingContext("value"))
      .withColumn("InternetConnectionType", 'InternetConnectionType("value"))
      .withColumn("OperatingSystem", 'OperatingSystem("value"))
      .withColumn("DeviceType", 'DeviceType("value"))
      .withColumn(
        "AdWidthInPixels",
        when('AdWidthInPixels.isNotNull, ('AdWidthInPixels - lit(1.0)) / lit(9999.0)).otherwise(0)
      )
      .withColumn(
        "AdHeightInPixels",
        when('AdHeightInPixels.isNotNull, ('AdHeightInPixels - lit(1.0)) / lit(9999.0)).otherwise(0)
      )
      .withColumn(
        "Latitude",
        when('Latitude.isNotNull, ('Latitude + lit(90.0)) / lit(180.0)).otherwise(0)
      )
      .withColumn(
        "Longitude",
        when('Longitude.isNotNull, ('Longitude + lit(180.0)) / lit(360.0)).otherwise(0)
      )
      .withColumn("MatchedSegmentsLength", when('MatchedSegments.isNull,0.0).otherwise(size('MatchedSegments).cast(FloatType)))
      .withColumn("HasMatchedSegments", when('MatchedSegmentsLength > lit(0), 1).otherwise(0))
      .withColumn("UserSegmentCount", when('UserSegmentCount.isNull, 0.0).otherwise('UserSegmentCount.cast(FloatType)))
      .withColumn("IdTypesBitmap", idTypesBitmap)
      // sample weight
      .withColumn("SampleWeights", typedLit(Array(1.0f)))

      // convert id to long
      .transform(addGuidBits("BidRequestId"))
      .transform(addGuidBits("TDID"))
      .cache()
      .repartition(AudienceModelInputGeneratorConfig.bidImpressionRepartitionNumAfterFilter, 'TDID)

    val policyTable = AudienceModelPolicyReadableDataset(AudienceModelInputGeneratorConfig.model)
      .readSinglePartition(dateTime)(spark)
      .filter((col("CrossDeviceVendorId") === 0) && (col("IsActive") === true))
      .select("SourceId", "SyntheticId", "ActiveSize", "IsSensitive")

    // select only the online available seed
    val campaignSeed = CampaignSeedDataset().readPartition(date)
      .join(
        broadcast(policyTable),
        col("SeedId") === col("SourceId"),
        "inner"
      ).repartition(AudienceModelInputGeneratorConfig.bidImpressionRepartitionNumAfterFilter, 'CampaignId)

    val devicetype = DeviceTypeDataset().readPartition(date)(spark)

    val joinedImpressionsWithCampaign = bidsImpressions
      .join(campaignSeed, Seq("CampaignId"), "inner")
      //.withColumn("SyntheticIds", array($"SyntheticId".cast(IntegerType))) // Ensure SyntheticIds is a long array
      .withColumn("SyntheticIds", $"SyntheticId".cast(IntegerType)) // single element array will be convert to scalar value, so use scalar to save computing

    // use less columns for faster operation
    val simpleJoinedImpressionsWithCampaign = joinedImpressionsWithCampaign.select(
        'BidRequestId,
        'DeviceAdvertisingId,
        'CookieTDID,
        'UnifiedId2,
        'EUID,
        'IdentityLinkId,
        'SeedId,
        'SyntheticIds,
        'Site,
        'Zip,
        'AliasedSupplyPublisherId,
        'City,
        'IsSensitive)

    // joinOnIdTypes will explode the dataframe with idtypes
    val joinedImpressionsWithCampaignSeed = joinOnIdTypes(simpleJoinedImpressionsWithCampaign.toDF(), aggSeed.toDF(), "left")
      .withColumn(
        "Targets",
        when(array_contains($"SeedIds", $"SeedId"), lit(1.0f))
          .otherwise(
            when($"SeedIds".isNull, lit(0.0f))
              .otherwise(lit(0.0f))
          )
      )
      // .withColumn(
      //   "Targets",
      //   when(array_contains($"SeedIds", $"SeedId"), array(lit(1.0f)))
      //     .otherwise(
      //       when($"SeedIds".isNull, array(lit(0.0f)))
      //         .otherwise(array(lit(0.0f)))
      //     )
      // )
      // currently, we only extend seed size less than 3M, should consider whether need to add more
      .withColumn(
        "PersonGraphTargets",
        when(array_contains($"PersonGraphSeedIds", $"SeedId"), lit(1.0f))
          .otherwise(
            when($"PersonGraphSeedIds".isNull, lit(0.0f))
              .otherwise(lit(0.0f))
          )
      )
      .withColumn(
        "HouseholdGraphTargets",
        when(array_contains($"HouseholdGraphSeedIds", $"SeedId"), lit(1.0f))
          .otherwise(
            when($"HouseholdGraphSeedIds".isNull, lit(0.0f))
              .otherwise(lit(0.0f))
          )
      )
      .withColumn(
        "PersonGraphTargets",
          greatest(
            col("PersonGraphTargets"),
            col("Targets")
          )
      )
      .withColumn(
        "HouseholdGraphTargets",
          greatest(
            col("HouseholdGraphTargets"),
            col("Targets")
          )
      )
      .cache()

    val feature_store_user = TDIDDensityScoreReadableDataset().readPartition(date.minusDays(1), subFolderKey = Some("split"), subFolderValue = Some(0 until 10))(spark)
      .select('TDID, 'SyntheticId_Level1, 'SyntheticId_Level2)

    val feature_store_seed = DailySeedDensityScoreReadableDataset().readPartition(date.minusDays(1))(spark).select('FeatureKey, 'FeatureValueHashed, 'SeedId, 'DensityScore)

    val bidsImpressionExtend = joinedImpressionsWithCampaignSeed
    .join(feature_store_user, Seq("TDID"), "left")
    .withColumn(
      "ZipSiteLevel_Seed",
      when(array_contains(col("SyntheticId_Level2"), col("SyntheticIds")), 3)
        .when(array_contains(col("SyntheticId_Level1"), col("SyntheticIds")), 2)
        .otherwise(1)
    )
    .drop("SyntheticId_Level1", "SyntheticId_Level2")
      .withColumn("SiteZipHashed", xxhash64(concat(concat(col("Site"), col("Zip")), lit(RSMV2UserSampleSalt))))
      .withColumn("AliasedSupplyPublisherIdCityHashed", xxhash64(concat(concat(col("AliasedSupplyPublisherId"), col("City")), lit(RSMV2UserSampleSalt))))
      .withColumn("FeatureKey", when(col("IsSensitive"), lit("AliasedSupplyPublisherIdCity")).otherwise(lit("SiteZip")))
      .withColumn("FeatureValueHashed", when(col("IsSensitive"), col("AliasedSupplyPublisherIdCityHashed")).otherwise(col("SiteZipHashed")))
      .join(feature_store_seed, Seq("FeatureKey", "FeatureValueHashed", "SeedId"), "left")
      .drop("FeatureKey", "FeatureValueHashed", "IsSensitive")
    .withColumn(
      "score_category",
      when((col("DensityScore") >= 0.8) && (col("DensityScore") < 0.99), 2)
      .when(col("DensityScore") >= 0.99, 3)
      .otherwise(1)
      )
    .withColumn(
      "ZipSiteLevel_Seed",
      greatest(col("score_category"), col("ZipSiteLevel_Seed"))
      )
      .drop("score_category", "DensityScore")

    // aggregate the targets and density features with max logic and join back with features
    // not use tdid from joinOnIdTypes -> will duplicate the info here
    val bidsImpressionExtendAgg = bidsImpressionExtend.groupBy("BidRequestId", "SiteZipHashed", "AliasedSupplyPublisherIdCityHashed")
      .agg(array(max("Targets")).as("Targets"),
      array(max("PersonGraphTargets")).as("PersonGraphTargets"),
      array(max("HouseholdGraphTargets")).as("HouseholdGraphTargets"),
      array(max("ZipSiteLevel_Seed")).as("ZipSiteLevel_Seed"))

    // get the full features back for each bidrequest
    val bidsImpressionExtendLabeled = generateContextualFeatureTier1(joinedImpressionsWithCampaign.join(bidsImpressionExtendAgg, Seq("BidRequestId"), "inner")
                                      .withColumn("MatchedSegmentsLength", when('MatchedSegments.isNull,0.0).otherwise(size('MatchedSegments).cast(FloatType)))
                                      .withColumn("HasMatchedSegments", when('MatchedSegments.isNull,0).otherwise(1))
                                      .withColumn("UserSegmentCount", when('UserSegmentCount.isNull, 0.0).otherwise('UserSegmentCount.cast(FloatType)))
                                      .join(devicetype, devicetype("DeviceTypeId") === joinedImpressionsWithCampaign("DeviceType"), "left")
                                      .withColumn("DeviceTypeName", when(col("DeviceTypeName").isNotNull, 'DeviceTypeName)
                                                      .otherwise("Unknown"))
                                      .withColumn("SyntheticIds", array('SyntheticIds))
                                      .drop("DeviceTypeId"))
                                      .repartition(AudienceModelInputGeneratorConfig.bidImpressionRepartitionNumAfterFilter, 'TDID)

    val rawJson = readModelFeatures(featuresJsonSourcePath)()

    val bidsImpressionsTrasnformed = castDoublesToFloat(bidsImpressionExtendLabeled)

    val result = ModelFeatureTransform.modelFeatureTransform[RelevanceOnlineRecord](bidsImpressionsTrasnformed, rawJson)

    val resultSet = paddingColumns(result.toDF(), conf.paddingColumns, 0).cache()

    val subFolderKey = conf.subFolderKey
    val subFolderValue = conf.subFolderValue

    // very important: tfrecord will auto convert single element array to a scalar
    RelevanceOnlineDatasetWithExperiment(conf.model, confettiEnv, experimentName, conf.tag, version=1)
      .writePartition(
        resultSet.as[RelevanceOnlineRecord],
        dateTime,
        saveMode = SaveMode.Overwrite,
        format = Some("tfrecord"),
        subFolderKey = Some(subFolderKey),
        subFolderValue = Some(subFolderValue)
        )

    RelevanceOnlineDatasetWithExperiment(conf.model, confettiEnv, experimentName, conf.tag, version=2)
      .writePartition(
        resultSet.as[RelevanceOnlineRecord],
        dateTime,
        saveMode = SaveMode.Overwrite,
        format = Some("cbuffer"),
        subFolderKey = Some(subFolderKey),
        subFolderValue = Some(subFolderValue)
        )

    onlineRelevanceBiddingTableSize.labels(dateTime.toLocalDate.toString).set(result.count())
    jobRunningTime.labels(dateTime.toLocalDate.toString).set(System.currentTimeMillis() - start)
  }
}