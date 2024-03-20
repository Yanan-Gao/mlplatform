package job

import com.thetradedesk.kongming.{date, _}
import com.thetradedesk.kongming.datasets._
import com.thetradedesk.spark.TTDSparkContext.spark.implicits._
import com.thetradedesk.spark.util.TTDConfig.config
import com.thetradedesk.spark.sql.SQLFunctions._
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._
import org.apache.spark.sql.Encoder
import org.apache.spark.sql.Dataset

import java.time.LocalDate


object GenerateWatchlistData extends KongmingBaseJob {

  override def jobName: String = "GenerateWatchlistData"

  val policyTrainsetSampleRate = config.getDouble("policyTrainsetSampleRate", 0.01)
  val policyOosSampleRate = config.getDouble("policyOosSampleRate", 0.02)
  val negSampleRate = config.getDouble("negSampleRate", 0.002)
  val capImpressionCnt = config.getInt("capImpressionCnt", 100000)
  val oosShortDelay = config.getInt("oosShortDelay", 3)
  val oosLongDelay = config.getInt("oosLongDelay", 10)
  val incTrain = config.getBoolean("incTrain", true)
  val defaultDate = LocalDate.of(2022, 3, 15)

  def generateMappings(date: LocalDate, sampleRate: Double): Dataset[_] = {
    // get mappings from all adgroups under the advertiser in watchlist + randomly sampled policy
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

  def downsampleNegatives[T: Encoder](impDf: Dataset[T],
                                      negSampleRate: Double,
                                      targetCol: String = "Target",
                                      groupCol: String = "AdGroupId"): Dataset[T] = {
    // down-sample oos negatives only for more efficient scoring & evaluation in later steps
    // down-sample to ensure final pos/neg ratio to >= `negSampleRate`, don't sample when pos/neg ratio >= `negSampleRate`
    val window = Window.partitionBy(groupCol)

    impDf.withColumn("posCnt", sum(targetCol).over(window))
      .withColumn("negCnt", sum(lit(1) - col(targetCol)).over(window))
      .withColumn("posNegRatio", $"posCnt" / $"negCnt")
      .withColumn("samplingRate", $"posNegRatio" / lit(negSampleRate))
      .withColumn("Rand", rand(seed = samplingSeed))
      .filter(!(
        (col(targetCol) === lit(0)) && ($"posNegRatio" <= lit(negSampleRate))
          && ($"Rand" > $"samplingRate")
      )).selectAs[T]

  }

  def capTotalImpression[T: Encoder](
                                    impDf: Dataset[T],
                                    capCount: Int,
                                    targetCol: String = "Target",
                                    capCol: String = "AdGroupId",
                                    ): Dataset[T] = {

    val capWindow = Window.partitionBy(capCol)

    impDf.withColumn("impCnt", count(targetCol).over(capWindow))
      .withColumn("Rand", rand(seed = samplingSeed))
      .withColumn("sampleRate", lit(capCount) / $"impCnt")
      .filter($"Rand" <= $"sampleRate")
      .withColumn("posCnt", sum(targetCol).over(capWindow))
      .filter($"posCnt" > lit(0))
      .selectAs[T]
  }


  override def runTransform(args: Array[String]): Array[(String, Long)] = {
    // get train & validation: use today policy table
    val watchlistMappingToDate = generateMappings(date, policyTrainsetSampleRate)

    // get trainset splits & oos with two types of short & long delays
    val splits = Seq("train", "val")
    val delays = Seq((oosShortDelay, s"delay_${oosShortDelay}d"), (oosLongDelay, s"delay_${oosLongDelay}d"))

    // generate watchlist trainset data
    val csvDS = if (incTrain) DataIncCsvForModelTrainingDataset() else DataCsvForModelTrainingDataset()
    val trainDF = csvDS.readPartition(date)

    val watchlistTrainDF = trainDF.drop("AdGroupIdStr", "CampaignIdStr", "AdvertiserIdStr").join(
      broadcast(watchlistMappingToDate),
      Seq("AdGroupId", "CampaignId", "AdvertiserId"),
      "inner"
    )
    val trainsetRows = splits.par.map { split =>
      val sampledWatchlistTrainDF =
        capTotalImpression[OutOfSampleAttributionRecord](
          watchlistTrainDF
            .filter(col("split") === lit(split)).selectAs[OutOfSampleAttributionRecord](nullIfAbsent = true),
          capCount = capImpressionCnt,
          capCol = "AdGroupId"
        )
      WatchListDataForEvalDataset().writePartition(
        sampledWatchlistTrainDF, date, split, Some(partCount.WatchlistTrainset)
      )
    }

    val oosRows = delays.par.map { case (delay, split) =>

      val dDate = date.minusDays(delay)
      val watchlistMappings = generateMappings(dDate, policyOosSampleRate)

      val watchlistData = OutOfSampleAttributionDataset(delay).readDate(dDate).join(
        broadcast(watchlistMappings),
        Seq("AdGroupIdStr", "CampaignIdStr", "AdvertiserIdStr"),
        "left_semi"
      )

      // downsample negatives to pos/neg ratio = 0.002 per campaign;
      val oosDf = downsampleNegatives[OutOfSampleAttributionRecord](
        watchlistData.selectAs[OutOfSampleAttributionRecord](nullIfAbsent = true),
        negSampleRate = negSampleRate,
        groupCol = "CampaignIdStr"
      )

      WatchListOosDataForEvalDataset().writePartition(oosDf, dDate, split, Some(partCount.WatchlistOOS))
    }

    (trainsetRows ++ oosRows).toArray

  }
}
