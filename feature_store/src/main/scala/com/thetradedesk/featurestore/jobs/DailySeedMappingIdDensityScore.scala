package com.thetradedesk.featurestore.jobs

import com.thetradedesk.featurestore.rsm.CommonEnums
import com.thetradedesk.featurestore.{MLPlatformS3Root, ProvisioningDatasetS3Root, date, ttdEnv}
import com.thetradedesk.featurestore.rsm.CommonEnums.DataSource
import com.thetradedesk.spark.TTDSparkContext.spark
import com.thetradedesk.spark.TTDSparkContext.spark.implicits._
import com.thetradedesk.spark.util.TTDConfig.config
import org.apache.spark.sql.{DataFrame, SaveMode}
import org.apache.spark.sql.functions._

import java.time.LocalDate
import scala.util.Random

object DailySeedMappingIdDensityScore extends DensityFeatureBaseJob {
  override val jobName: String = "DailySeedMappingIdDensityScore"

  private def readReIndexedSeedDensity(date: LocalDate) = {
    spark.read.parquet(s"$MLPlatformS3Root/prod/profiles/source=bidsimpression/index=FeatureKeyValue/job=DailyDensityScoreReIndexingJob/config=SyntheticIdDensityScoreCategorized/date=${getDateStr(date)}")
  }

  private def readCampaignSeeds(date: LocalDate) = {
    spark.read.parquet(s"$ProvisioningDatasetS3Root/campaignseed/v=1/date=${getDateStr(date)}")
  }

  override def runTransform(args: Array[String]): Unit = {
    val dateStr = getDateStr(date)
    // get configurations
    val maxNumMappingIdsInAerospike = config.getInt("maxNumMappingIdsInAerospike", default = 1500)

    // read source datasets
    val seedDensity = readReIndexedSeedDensity(date)
    val campaignSeeds = readCampaignSeeds(date)
    val policyTable = readPolicyTable(date.minusDays(1), DataSource.Seed.id)

    // filter active seeds
    val activeCampaigns = readActiveCampaigns(date)

    val seedMappingIdDensityScores = transform(
      seedDensity,
      campaignSeeds,
      policyTable,
      activeCampaigns,
      maxNumMappingIdsInAerospike
    )

    val writePath = s"$MLPlatformS3Root/$ttdEnv/profiles/source=bidsimpression/index=FeatureKeyValue/job=$jobName/v=1/date=$dateStr"
    seedMappingIdDensityScores.coalesce(4096).write.mode(SaveMode.Overwrite).parquet(writePath)
  }

  def transform(
                 seedDensity: DataFrame,
                 campaignSeeds: DataFrame,
                 policyTable: DataFrame,
                 activeCampaigns: DataFrame,
                 maxNumMappingIdsInAerospike: Int
               ): DataFrame = {
    val activeSeeds = campaignSeeds
      .join(activeCampaigns, Seq("CampaignId"), "leftsemi")
      .select($"SeedId")
      .distinct()

    val activeSyntheticIdToMappingId = spark.sparkContext.broadcast(
      policyTable
        .select($"SyntheticId", $"MappingId", $"SeedId")
        .join(activeSeeds, Seq("SeedId"))
        .select($"SyntheticId", $"MappingId")
        .as[(Int, Int)]
        .collect()
        .toMap
    )

    // udf to
    // 1. filter active seeds
    // 2. convert synthetic id to mapping id
    val SyntheticIdToActiveMappingIdUdf = udf(
      (syntheticIds: Seq[Int], maxLength: Int) => {
        val transformed = syntheticIds
          .filter(syntheticId => activeSyntheticIdToMappingId.value.contains(syntheticId))
          .flatMap(activeSyntheticIdToMappingId.value.get)

        if (transformed.length <= maxLength) transformed
        else Random.shuffle(transformed).take(maxLength)
      }
    )

    val featureKeyToEnum = udf((featureKey: String) => {
      CommonEnums.FeatureKey.withName(featureKey).id
    })

    splitDensityScoreSlots(
      seedDensity
        .withColumn("MappingIdLevel2", SyntheticIdToActiveMappingIdUdf($"SyntheticIdLevel2", lit(maxNumMappingIdsInAerospike)))
        .withColumn("MappingIdLevel1", SyntheticIdToActiveMappingIdUdf($"SyntheticIdLevel1", lit(maxNumMappingIdsInAerospike) - size($"MappingIdLevel2")))
        .filter(size($"MappingIdLevel1") + size($"MappingIdLevel2") > 0)
    )
      .withColumn("FeatureKeyEnum", featureKeyToEnum($"FeatureKey"))
      .withColumn("FeatureKeyValueHashed", concat_ws("_", $"FeatureKeyEnum".cast("string"), $"FeatureValueHashed".cast("string")))
      .select($"FeatureKeyValueHashed", $"MappingIdLevel1", $"MappingIdLevel1S1", $"MappingIdLevel2", $"MappingIdLevel2S1")
  }
}
