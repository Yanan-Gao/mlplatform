package com.thetradedesk.audience.jobs

import com.thetradedesk.audience.configs.AudienceModelInputGeneratorConfig
import com.thetradedesk.geronimo.shared.transform.ModelFeatureTransform
import com.thetradedesk.audience.datasets._
import com.thetradedesk.audience.transform.ContextualTransform.generateContextualFeatureTier1
import com.thetradedesk.audience.utils.Logger.Log
import com.thetradedesk.audience.{date, dateTime, _}
import com.thetradedesk.geronimo.shared.readModelFeatures
import com.thetradedesk.audience.jobs.HitRateReportingTableGeneratorJob.prometheus
import com.thetradedesk.audience.jobs.modelinput.rsmv2.RelevanceModelInputGeneratorConfig.RSMV2UserSampleSalt
import com.thetradedesk.geronimo.bidsimpression.schema.{BidsImpressions, BidsImpressionsSchema}
import com.thetradedesk.geronimo.shared.{GERONIMO_DATA_SOURCE, loadParquetData}
import com.thetradedesk.spark.TTDSparkContext.spark
import com.thetradedesk.spark.TTDSparkContext.spark.implicits._
import com.thetradedesk.spark.util.TTDConfig.{config, defaultCloudProvider}
import com.thetradedesk.spark.util.io.FSUtils
import com.thetradedesk.spark.util.prometheus.PrometheusClient
import org.apache.commons.lang3.exception.ExceptionUtils
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.apache.spark.sql.{DataFrame, Dataset, Row, SaveMode, SparkSession}
import com.thetradedesk.audience.jobs.modelinput.rsmv2.usersampling.SamplerFactory

import java.time.format.DateTimeFormatter
import java.time.temporal.ChronoUnit
import java.time.{LocalDate, LocalDateTime}
import scala.util.Random

object RelevanceOnlineBiddingDataGeneratorJob {
  val prometheus = new PrometheusClient("ModelQualityJob", "RelevanceOnlineBiddingDataGeneratorJob")

  def main(args: Array[String]): Unit = {
    runETLPipeline()
    prometheus.pushMetrics()
  }

  def runETLPipeline(): Unit = {
    new RelevanceOnlineBiddingDataGenerator(prometheus).generate(date)

  }
}


class RelevanceOnlineBiddingDataGenerator(prometheus: PrometheusClient) {

  val jobRunningTime = prometheus.createGauge(s"relevance_online_bidding_generation_job_running_time", "RelevanceOnlineBiddingDataGenerator running time", "date")
  val onlineRelevanceBiddingTableSize = prometheus.createGauge(s"online_relevance_bidding_table_size", "OnlineRelevanceBiddingTableGenerator table size", "date")
  val sampler = SamplerFactory.fromString("RSMV2Measurement")

  def generate(date: LocalDate): Unit = {

    val dateTime = date.atStartOfDay()

    val start = System.currentTimeMillis()

    val aggSeed = AggregatedSeedReadableDataset()
      .readPartition(date)(spark)
      .filter(sampler.samplingFunction('TDID))
      .repartition(AudienceModelInputGeneratorConfig.bidImpressionRepartitionNumAfterFilter, 'TDID)
      .cache()

    val bidImpressionsS3Path = BidsImpressions.BIDSIMPRESSIONSS3 + "prod/bidsimpressions/"
    val bidsImpressions = loadParquetData[BidsImpressionsSchema](bidImpressionsS3Path, date, lookBack = Some(0), source = Some(GERONIMO_DATA_SOURCE))
      .filter('IsImp)
      .withColumn("TDID", getUiid('UIID, 'UnifiedId2, 'EUID, 'IdType))
      .withColumn("TDID", when(col("TDID") === "00000000-0000-0000-0000-000000000000", null).otherwise(col("TDID")))
      .filter(col("TDID").isNotNull)
      .filter(sampler.samplingFunction('TDID))
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
        'RelevanceScore
      )
      .filter('RelevanceScore.isNotNull && 'RelevanceScore >= 0 && 'RelevanceScore <= 1)
      .withColumnRenamed("RelevanceScore", "OnlineRelevanceScore")
      .withColumn("OnlineRelevanceScore", col("OnlineRelevanceScore").cast("double"))
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
      .withColumn("MatchedSegmentsLength", when('MatchedSegments.isNull,0.0).otherwise(size('MatchedSegments).cast(DoubleType)))
      .withColumn("HasMatchedSegments", when('MatchedSegments.isNull,0).otherwise(1))
      .withColumn("UserSegmentCount", when('UserSegmentCount.isNull, 0.0).otherwise('UserSegmentCount.cast(DoubleType)))
      .repartition(AudienceModelInputGeneratorConfig.bidImpressionRepartitionNumAfterFilter, 'TDID)
      .cache()

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

    val joinedImpressionsWithCampaignSeed = bidsImpressions
      .join(campaignSeed, Seq("CampaignId"), "inner")
      //.withColumn("SyntheticIds", array($"SyntheticId".cast(IntegerType))) // Ensure SyntheticIds is a long array
      .withColumn("SyntheticIds", $"SyntheticId".cast(IntegerType)) // single element array will be convert to scalar value, so use scalar to save computing
      .join(aggSeed, Seq("TDID"), "left")
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
      .withColumn("MatchedSegmentsLength", when('MatchedSegments.isNull,0.0).otherwise(size('MatchedSegments).cast(DoubleType)))
      .withColumn("HasMatchedSegments", when('MatchedSegments.isNull,0).otherwise(1))
      .withColumn("UserSegmentCount", when('UserSegmentCount.isNull, 0.0).otherwise('UserSegmentCount.cast(DoubleType)))
      .join(devicetype, devicetype("DeviceTypeId") === bidsImpressions("DeviceType"), "left")
      .withColumn("DeviceTypeName", when(col("DeviceTypeName").isNotNull, 'DeviceTypeName)
                       .otherwise("Unknown"))
      .drop("DeviceTypeId")
      .repartition(AudienceModelInputGeneratorConfig.bidImpressionRepartitionNumAfterFilter, 'TDID)
      .cache()

    val feature_store_user = TDIDDensityScoreReadableDataset().readPartition(date.minusDays(1), subFolderKey = Some("split"), subFolderValue = Some("1"))(spark)
                              .select('TDID, 'SyntheticId_Level1, 'SyntheticId_Level2)

    val feature_store_seed = DailySeedDensityScoreReadableDataset().readPartition(date.minusDays(1))(spark).select('FeatureKey, 'FeatureValueHashed, 'SeedId, 'DensityScore)

    val bidsImpressionExtend = generateContextualFeatureTier1(joinedImpressionsWithCampaignSeed)
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

    val rawJson = readModelFeatures(featuresJsonSourcePath)()

    val result = ModelFeatureTransform.modelFeatureTransform[RelevanceOnlineRecord](bidsImpressionExtend, rawJson)

    val subFolderKey = config.getString("subFolderKey", default = "split")
    val subFolderValue = config.getString("subFolderValue", default = "OOS")

    // very important: tfrecord will auto convert single element array to a scalar
    RelevanceOnlineDataset(config.getString("Model", default = "RSMV2"), config.getString("tag", default = "Seed_None"), config.getInt("version", default = 1))
      .writePartition(
        result,
        dateTime,
        saveMode = SaveMode.Overwrite,
        format = Some("tfrecord"),
        subFolderKey = Some(subFolderKey),
        subFolderValue = Some(subFolderValue)
        )

    onlineRelevanceBiddingTableSize.labels(dateTime.toLocalDate.toString).set(result.count())
    jobRunningTime.labels(dateTime.toLocalDate.toString).set(System.currentTimeMillis() - start)
  }
}