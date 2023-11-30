package job

import com.thetradedesk.kongming._
import com.thetradedesk.kongming.datasets._
import com.thetradedesk.spark.TTDSparkContext.spark.implicits._
import com.thetradedesk.spark.util.TTDConfig.config
import com.thetradedesk.spark.sql.SQLFunctions._
import com.thetradedesk.spark.sql.SQLFunctions.DataSetExtensions
import org.apache.spark.sql.functions._
import job.KongmingBaseJob

import java.time.LocalDate


object GenerateWatchlistData extends KongmingBaseJob {

  override def jobName: String = "GenerateWatchlistData"

  override def runTransform(args: Array[String]): Array[(String, Long)] = {

    val policySampleRate = config.getDouble("policySampleRate", 0.01)
    val oosShortDelay = config.getInt("oosShortDelay", 3)
    val oosLongDelay = config.getInt("oosLongDelay", 10)
    val incTrain = config.getBoolean("incTrain", true)
    val defaultDate = LocalDate.of(2022, 3, 15)

    // get mappings from all adgroups under the advertiser
    val watchlistManual = WatchListDataset().readDate(defaultDate)
    val adGroupPolicy = AdGroupPolicyDataset().readDate(date)
    val policyMappings = AdGroupPolicyMappingDataset().readDate(date)
    // sample full policy table
    val sampledPolicy = adGroupPolicy.filter(rand(seed = samplingSeed) < lit(policySampleRate))
      .select("ConfigKey", "ConfigValue")

    val watchlistDF = sampledPolicy.union(
      multiLevelJoinWithPolicy[AdGroupPolicyMappingRecord](
        policyMappings, watchlistManual, "left_semi", joinKeyName = "Level", joinValueName = "Id"
      ).select("ConfigKey", "ConfigValue")
    ).dropDuplicates()

    val watchlistMappings = policyMappings.join(watchlistDF, Seq("ConfigKey", "ConfigValue"), "left_semi")
      .withColumnRenamed("AdGroupId", "AdGroupIdStr").withColumnRenamed("AdGroupIdInt", "AdGroupId")
      .withColumnRenamed("CampaignId", "CampaignIdStr").withColumnRenamed("CampaignIdInt", "CampaignId")
      .withColumnRenamed("AdvertiserId", "AdvertiserIdStr").withColumnRenamed("AdvertiserIdInt", "AdvertiserId")

    // get train & validation
    val splits = Seq("train", "val")
    val csvDS = if (incTrain) DataIncCsvForModelTrainingDataset() else DataCsvForModelTrainingDataset()

    val trainDF = csvDS.readPartition(date)
    val watchlistTrainDF = trainDF.join(
      broadcast(watchlistMappings),
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
      val oosDf = OutOfSampleAttributionDataset().readDate(dDate).join(
        broadcast(watchlistMappings),
        Seq("AdGroupIdStr", "CampaignIdStr", "AdvertiserIdStr"),
        "left_semi"
      ).selectAs[OutOfSampleAttributionRecord](nullIfAbsent = true)

      WatchListOosDataForEvalDataset().writePartition(oosDf, dDate, split, Some(partCount.WatchlistOOS))

    }

    (trainsetRows ++ oosRows).toArray

  }
}
