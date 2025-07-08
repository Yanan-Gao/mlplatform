package com.thetradedesk.featurestore.jobs

import com.thetradedesk.featurestore.datasets.metadata.DaRestrictedAdvertiserDataset
import com.thetradedesk.featurestore.rsm.CommonEnums.DataSource
import com.thetradedesk.featurestore.utils.{S3Utils, SeedPolicyUtils}
import com.thetradedesk.featurestore.{MLPlatformS3Root, date, ttdEnv}
import com.thetradedesk.spark.TTDSparkContext.spark
import com.thetradedesk.spark.TTDSparkContext.spark.implicits._
import com.thetradedesk.spark.datasets.core.AnnotatedSchemaBuilder
import com.thetradedesk.spark.datasets.sources.{CampaignDataSet, SeedRecord}
import com.thetradedesk.spark.datasets.sources.provisioning.CampaignFlightDataSet
import com.thetradedesk.spark.util.TTDConfig.config
import com.thetradedesk.spark.util.io.FSUtils
import org.apache.hadoop.fs.PathNotFoundException
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{DataFrame, SaveMode}

import java.sql.Timestamp
import scala.concurrent.duration._
import java.time.LocalDate
import java.time.format.DateTimeFormatter

object DailyNewSeedFeaturePairDensityScore extends DensityFeatureBaseJob {
  override val jobName: String = "DailyNewSeedFeaturePairDensityScore"
  val jobConfigName: String = "DailyNewSeedDensityScore"
  val doNotTrackTDID: String = "00000000-0000-0000-0000-000000000000"
  val seedDetailsTimestampFormat = "yyyy-MM-dd'T'HH:mm:ss.SSSSSS"

  val seedProcessLowerThreshold = config.getLong("seedProcessLowerThreshold", 2000)
  val seedProcessUpperThreshold = config.getLong("seedProcessUpperThreshold", 100000000)
  val maxNewSeedCountThreshold = config.getInt("maxNewSeedCountThreshold", default = 2000)
  val seedMetadataS3Path = S3Utils.refinePath(config.getString("seedMetadataS3Path", "prod/data/SeedDetail/v=1/"))
  val seedDataS3Path = S3Utils.refinePath(config.getString("seedDataS3Path", "prod/data/Seed/v=1/"))
  val seedS3Bucket = S3Utils.refinePath(config.getString("seedMetadataS3Bucket", "ttd-datprd-us-east-1"))
  val seedMetaDataRecentVersion = config.getString("seedMetaDataRecentVersion", null)

  val seedGroupFeatureKeys = config.getString("seedGroupFeatureKeys", "Site,Zip").split(",").toSeq
  val supportIdTypeCols = config.getString("supportIdTypeCols", "UIID").split(",").toSeq // CookieTDID,DeviceAdvertisingId,UnifiedId2,EUID
  val isSensitive = config.getBoolean("IsSensitive", false)
  val readSeedDetailMode = config.getBoolean("readSeedDetailMode", false)
  val newSeedRecencyDays = config.getInt("newSeedRecencyDays", 3)


  val densityFeatureScoreNewSeedPrefix = config.getString("densityFeatureScoreNewSeedPath",
    s"$MLPlatformS3Root/$ttdEnv/profiles/source=bidsimpression/index=SeedId/config=$jobConfigName${seedGroupFeatureKeys.mkString("")}/v=1")
  val densityFeatureScoreNewSeedMetadataPrefix = config.getString("densityFeatureScoreNewSeedPath",
    s"$MLPlatformS3Root/$ttdEnv/profiles/source=bidsimpression/index=SeedId/metadata/newSeed${seedGroupFeatureKeys.mkString("")}/v=1")

  val activeAdvertiserLookBackDays = config.getInt("activeAdvertiserLookBackDays", 180)
  var newSeedLookBackDays = config.getInt("newSeedLookBackDays", 7)

  def findNewSeed(latestSeedDetailPath: String) = {
    val yesterday = date.minusDays(1)

    val restrictedAdvertisers = DaRestrictedAdvertiserDataset().readDate(date)

    val policyTableToday = getEligibleSeeds(latestSeedDetailPath)
      .select("SeedId", "AdvertiserId", "Count")
      .join(restrictedAdvertisers, Seq("AdvertiserId"), "left")
      .withColumn(
        "IsSensitive",
        when(col("IsRestricted") === 1, lit(true)).otherwise(false)
      )
      .select("SeedId", "IsSensitive")

    val policyTableYest = readPolicyTable(yesterday, DataSource.Seed.id)
      .select("SeedId")

    policyTableToday.join(policyTableYest, Seq("SeedId"), "left_anti").select("SeedId", "IsSensitive")
      .filter('IsSensitive === isSensitive)
  }

  // seeds under active advertiser or recently created seeds
  def getEligibleSeeds(latestSeedDetailPath: String): DataFrame = {
    val activeAdvertiserStartingDateStr = DateTimeFormatter
      .ofPattern("yyyy-MM-dd 00:00:00")
      .format(date.minusDays(activeAdvertiserLookBackDays))

    val campaignFlights = CampaignFlightDataSet().readDate(date)
    val campaigns = CampaignDataSet().readDate(date)

    val activeAdvertisers = campaignFlights
      .filter('EndDateExclusiveUTC.isNull || 'EndDateExclusiveUTC.gt(activeAdvertiserStartingDateStr))
      .select('CampaignId)
      .distinct()
      .join(campaigns, Seq("CampaignId"))
      .select('AdvertiserId)
      .distinct()

    val newSeedTimestamp = Timestamp.valueOf(date.atStartOfDay().minusDays(newSeedLookBackDays))

    spark.read.parquet(latestSeedDetailPath)
      .join(activeAdvertisers.as("a"), Seq("AdvertiserId"), "left")
      .filter($"a.AdvertiserId".isNotNull || to_timestamp('CreatedAt, "yyyy-MM-dd'T'HH:mm:ss.SSSSSS").gt(lit(newSeedTimestamp)))
      .filter('Count <= seedProcessUpperThreshold && 'Count >= seedProcessLowerThreshold)
  }

  def aggregateNewSeed(newSeedIds: DataFrame, seedDetailPath: String) = {

    val seedDataPaths = spark.read.parquet(seedDetailPath).join(newSeedIds, "SeedId")
      .select("Path").as[String].collect()
    val basePath = "s3://" + seedS3Bucket + "/" + seedDataS3Path

    spark.read.option("basePath", basePath)
      .schema(AnnotatedSchemaBuilder.schema[SeedRecord])
      .parquet(seedDataPaths: _*)
      .select(col("UserId").alias("TDID"), col("SeedId"))
      .groupBy(col("TDID"))
      .agg(collect_set(col("SeedId")).alias("SeedIds"))
      .cache()
  }

  def loadInputData(date: LocalDate) = {
    val dateStr = getDateStr(date)
    val yyyy = dateStr.substring(0, 4)
    val mm = dateStr.substring(4, 6)
    val dd = dateStr.substring(6, 8)

    val seedGroupFeatureCondition = seedGroupFeatureKeys
      .map(c => col(c).isNotNull)
      .reduce(_ && _)

    val supportIdCondition = supportIdTypeCols
      .map(c => col(c).isNotNull && col(c) =!= doNotTrackTDID)
      .reduce(_ || _)

    val df = spark.read.parquet(s"s3://thetradedesk-mlplatform-us-east-1/features/data/koav4/v=1/prod/bidsimpressions/year=$yyyy/month=$mm/day=$dd/")
      .select((supportIdTypeCols ++ seedGroupFeatureKeys :+ "BidRequestId").map(col): _*)
      .filter(seedGroupFeatureCondition)
      .filter(supportIdCondition)

    val hashedColumn = xxhash64(concat(concat_ws("", seedGroupFeatureKeys.map(col): _*), lit(salt))).cast("long")

    df.withColumn("FeatureValueHashed", hashedColumn)
  }

  def readSeedDetailPath(seedDetailFilePath: String) = {
    if (readSeedDetailMode) {
      val maxRetries = 3
      var attempt = 0
      var resultOpt: Option[String] = None

      while (attempt < maxRetries && resultOpt.isEmpty) {
        try {
          val raw = FSUtils.readStringFromFile(seedDetailFilePath)(spark)
          resultOpt = Some(raw.trim)
        } catch {
          case _: PathNotFoundException =>
            attempt += 1
            if (attempt < maxRetries) Thread.sleep(3.minutes.toMillis)
        }
      }

      val path = resultOpt.getOrElse {
        throw new IllegalStateException(
          s"Could not read seed detail path after $maxRetries attempts from $seedDetailFilePath"
        )
      }

      // validate S3 URI
      if (!path.matches("^s3[an]?://[^/]+/.+"))
        throw new IllegalArgumentException(s"Invalid S3 path read from metadata: $path")

      path
    } else {
      // fetch latest if not in read mode
      SeedPolicyUtils.getRecentVersion(seedS3Bucket, seedMetadataS3Path, seedMetaDataRecentVersion)
    }
  }


  override def runTransform(args: Array[String]): Unit = {
    val dateStr = getDateStr(date)
    val siteZipScoreNewSeedPath = s"${densityFeatureScoreNewSeedPrefix}/date=${dateStr}"
    val siteZipScoreNewSeedMetadataPath = s"${densityFeatureScoreNewSeedMetadataPrefix}/date=${dateStr}"
    val seedDetailFilePath = s"$siteZipScoreNewSeedMetadataPath/_SEED_DETAIL_PATH"

    val seedDetailPath = readSeedDetailPath(seedDetailFilePath)

    if (!readSeedDetailMode) {
      FSUtils.writeStringToFile(s"$siteZipScoreNewSeedMetadataPath/_SEED_DETAIL_PATH", seedDetailPath)(spark)
      //if already exit throw error
    }

    val newSeedIds = findNewSeed(seedDetailPath)
    val newSeedCount = newSeedIds.count()

    if (newSeedCount == 0) {
      FSUtils.writeStringToFile(s"$siteZipScoreNewSeedMetadataPath/_EMPTY", "")(spark)
    } else if (newSeedCount > maxNewSeedCountThreshold) {
      FSUtils.writeStringToFile(s"$siteZipScoreNewSeedMetadataPath/_OVER_MAX", "")(spark)
    }
    else {
      val aggregatedNewSeed = aggregateNewSeed(newSeedIds, seedDetailPath)
      val bidreq = loadInputData(date)

      // same as
      // bidreq("DeviceAdvertisingId") === aggregatedNewSeed("TDID") ||
      // bidreq("CookieTDID") === aggregatedNewSeed("TDID") || ...
      val joinCondition = supportIdTypeCols
        .map(c => bidreq(c) === aggregatedNewSeed("TDID"))
        .reduce(_ || _)

      val seedDensity =
        bidreq.join(aggregatedNewSeed, joinCondition)
          .groupBy('BidRequestId, 'FeatureValueHashed)
          .agg(
            array_distinct(flatten(collect_list(col("SeedIds")))).as("SeedIds")
          )
          .withColumn("SeedId", explode(col("SeedIds")))
          .groupBy("SeedId", "FeatureValueHashed").count()
          .withColumnRenamed("Count", "SeedCount")
          .withColumn("SeedTotalCount", sum("SeedCount").over(Window.partitionBy("SeedId")))
          .withColumn("InDensity", col("SeedCount") / col("SeedTotalCount"))

      val generalPopulationFrequencyMap = bidreq.groupBy("FeatureValueHashed").count()
      val totalCnt = generalPopulationFrequencyMap.agg(sum("count")).first().getLong(0)

      val featureKeyScoreSeedId = generalPopulationFrequencyMap
        .withColumnRenamed("Count", "PopCount")
        .join(seedDensity, "FeatureValueHashed")
        .withColumn("OutDensity", ('PopCount - 'SeedCount) / (lit(totalCnt) - 'SeedTotalCount))
        .withColumn("DensityScore", 'InDensity / ('InDensity + 'OutDensity))
        .select("FeatureValueHashed", "SeedId", "DensityScore")

      featureKeyScoreSeedId.write.mode(SaveMode.Overwrite).parquet(siteZipScoreNewSeedPath)
    }

    FSUtils.writeStringToFile(s"$siteZipScoreNewSeedMetadataPath/_SUCCESS", "")(spark)
  }
}
