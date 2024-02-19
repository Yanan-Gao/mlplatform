package job

import com.thetradedesk.kongming.{date, _}
import com.thetradedesk.kongming.datasets._
import com.thetradedesk.spark.TTDSparkContext.spark.implicits._
import com.thetradedesk.spark.util.TTDConfig.config
import com.thetradedesk.spark.sql.SQLFunctions._
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._
import org.apache.spark.sql.Dataset

import java.time.LocalDate


object GenerateWatchlistData extends KongmingBaseJob {

  override def jobName: String = "GenerateWatchlistData"

  val policyTrainsetSampleRate = config.getDouble("policyTrainsetSampleRate", 0.01)
  val policyOosSampleRate = config.getDouble("policyOosSampleRate", 0.02)
  val maxAdgroupImpCnt = config.getInt("maxAdgroupImpCnt", 100000)
  val maxCampaignImpCnt = config.getInt("maxCampaignImpCnt", 500000)
  val minCampaignImpCnt = config.getInt("minCampaignImpCnt", 1000)
  val oosShortDelay = config.getInt("oosShortDelay", 3)
  val oosLongDelay = config.getInt("oosLongDelay", 10)
  val incTrain = config.getBoolean("incTrain", true)
  val defaultDate = LocalDate.of(2022, 3, 15)

  def generate_mappings(date: LocalDate, sampleRate: Double): Dataset[_] = {
    // get mappings from all adgroups under the advertiser
    val watchlistManual = WatchListDataset().readDate(defaultDate)
    val adGroupPolicy = AdGroupPolicyDataset().readDate(date)
    val policyMappings = AdGroupPolicyMappingDataset().readDate(date)
    // sample full policy table
    val sampledPolicy = adGroupPolicy.filter(rand(seed = samplingSeed) < lit(sampleRate))
      .select("ConfigKey", "ConfigValue")

    val watchlistDF = sampledPolicy.union(
      multiLevelJoinWithPolicy[AdGroupPolicyMappingRecord](
        policyMappings, watchlistManual, "left_semi", joinKeyName = "Level", joinValueName = "Id"
      ).select("ConfigKey", "ConfigValue")
    ).dropDuplicates()

    policyMappings.join(watchlistDF, Seq("ConfigKey", "ConfigValue"), "left_semi")
      .withColumnRenamed("AdGroupId", "AdGroupIdStr").withColumnRenamed("AdGroupIdInt", "AdGroupId")
      .withColumnRenamed("CampaignId", "CampaignIdStr").withColumnRenamed("CampaignIdInt", "CampaignId")
      .withColumnRenamed("AdvertiserId", "AdvertiserIdStr").withColumnRenamed("AdvertiserIdInt", "AdvertiserId")

  }

  override def runTransform(args: Array[String]): Array[(String, Long)] = {
    // get train & validation: use today policy table
    val watchlistMappingToDate = generate_mappings(date, policyTrainsetSampleRate)

    val splits = Seq("train", "val")
    val csvDS = if (incTrain) DataIncCsvForModelTrainingDataset() else DataCsvForModelTrainingDataset()

    val trainDF = csvDS.readPartition(date)
    val watchlistTrainDF = trainDF.drop("AdGroupIdStr", "CampaignIdStr", "AdvertiserIdStr").join(
      broadcast(watchlistMappingToDate),
      Seq("AdGroupId", "CampaignId", "AdvertiserId"),
      "inner"
    )
    val trainsetRows = splits.par.map { split =>
      WatchListDataForEvalDataset().writePartition(
        watchlistTrainDF.filter(col("split") === lit(split)).selectAs[OutOfSampleAttributionRecord](nullIfAbsent = true),
        date, split, Some(partCount.WatchlistTrainset)
      )
    }

    // get oos with two types of short & long delays
    val delays = Seq((oosShortDelay, s"delay_${oosShortDelay}d"), (oosLongDelay, s"delay_${oosLongDelay}d"))
    val oosRows = delays.par.map { case (delay, split) =>

      val dDate = date.minusDays(delay)
      val watchlistMappings = generate_mappings(dDate, policyOosSampleRate)

      val watchlistData = OutOfSampleAttributionDataset(delay).readDate(dDate).join(
        broadcast(watchlistMappings),
        Seq("AdGroupIdStr", "CampaignIdStr", "AdvertiserIdStr"),
        "left_semi"
      )

      // remove campaigns with no conversions, too few impressions, and down-sample large adgroups
      val oosDf = watchlistData.withColumn(
          "CampImpCnt", count("Target").over(Window.partitionBy("CampaignIdStr"))
        ).withColumn(
        "AgImpCnt", count("Target").over(Window.partitionBy("AdGroupIdStr"))
        ).withColumn("Rand", rand(seed = samplingSeed))
        .withColumn("AgRatio", lit(maxAdgroupImpCnt) / $"AgImpCnt")
        .withColumn("CpRatio", lit(maxCampaignImpCnt) / $"CampImpCnt")
        .filter($"Rand" <= $"AgRatio" && $"Rand" <= $"CpRatio")
        .withColumn(
          "CampPosCnt", sum("Target").over(Window.partitionBy("CampaignIdStr"))
        )
        .filter($"CampPosCnt" > 0 && $"CampImpCnt" > lit(minCampaignImpCnt))
        .selectAs[OutOfSampleAttributionRecord](nullIfAbsent = true)

      WatchListOosDataForEvalDataset().writePartition(oosDf, dDate, split, Some(partCount.WatchlistOOS))

    }

    (trainsetRows ++ oosRows).toArray

  }
}
