package job

import java.time.LocalDate

import com.thetradedesk.geronimo.bidsimpression.schema.BidsImpressions
import com.thetradedesk.frequency.schema.{ClickTrackerDataSet, FrequencyDailyAggregateDataSet, ClickBotUiidsDataSet, AdvertiserExclusionList}
import com.thetradedesk.frequency.transform.FrequencyDailyAggregationTransform
import com.thetradedesk.spark.TTDSparkContext.spark
import com.thetradedesk.geronimo.shared.explicitDatePart
import com.thetradedesk.spark.util.TTDConfig.config
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions._

object FrequencyDailyAggregation {

  private val DefaultPartitions = 200
  private val DefaultEnv = "prodTest"
  private val DefaultIsolatedCsv = AdvertiserExclusionList.ADVERTISEREXCLUSIONS3
  private val DefaultRoiGoalTypes = Seq(3, 4)
  def main(args: Array[String]): Unit = {
    val runDate = config.getDate("date", LocalDate.now())
    val env = config.getString("ttd.env", DefaultEnv)
    val (readEnv, writeEnv) = prodTestMapping(env)

    val partitions = config.getInt("partitions", DefaultPartitions)
    val roiGoalTypes = parseIntList(config.getString("roiGoalTypes", DefaultRoiGoalTypes.mkString(",")))
    val isolatedCsvPath = config.getString("isolatedCsv", AdvertiserExclusionList.ADVERTISEREXCLUSIONS3)
    val koaEnv = Option(config.getString("koaEnvOverwrite", readEnv))
      .map(_.trim)
      .filter(_.nonEmpty)
      .getOrElse(readEnv)

    val bidsImpressionsRoot = config.getString("bidsImpressionsRoot", BidsImpressions.BIDSIMPRESSIONSS3.stripSuffix("/"))
    val clickTrackerRoot = config.getString("clickTrackerRoot", ClickTrackerDataSet.CLICKSS3.stripSuffix("/"))
    val dailyAggRoot = config.getString("dailyAggOutputRoot", FrequencyDailyAggregateDataSet.root(writeEnv))
    val clickBotRoot = config.getString("clickBotUiidsRoot", ClickBotUiidsDataSet.root(writeEnv))

    val bidsImpressions = readBidsImpressions(spark, bidsImpressionsRoot, koaEnv, runDate)
    val clicks = readClicks(spark, clickTrackerRoot, runDate)
    val roiKeys = FrequencyDailyAggregationTransform.buildRoiAdGroupKeys(
      spark,
      runDate,
      roiGoalTypes
    )

    val isolated = readIsolatedAdvertisers(spark, isolatedCsvPath)

    val (dailyAggregates, clickBotUiids) = FrequencyDailyAggregationTransform.transform(
      bidsImpressions,
      clicks,
      roiKeys
    )

    val restricted = dailyAggregates.join(isolated, Seq("AdvertiserId"), "inner")
    val unrestricted = dailyAggregates.join(isolated, Seq("AdvertiserId"), "left_anti")

    val clickBotPartitions = math.max(partitions / 10, 1)
    writeDataset(clickBotUiids, clickBotRoot, "click_bot_uiids", runDate, clickBotPartitions)
    writeDataset(unrestricted, dailyAggRoot, "unrestricted", runDate, partitions)
    writeDataset(restricted, dailyAggRoot, "isolated", runDate, partitions)

    val normalizedDailyRoot = dailyAggRoot.stripSuffix("/")
    val normalizedClickBotRoot = clickBotRoot.stripSuffix("/")
    println(s"FrequencyDailyAggregation completed for date $runDate")
    println(s"  unrestricted -> $normalizedDailyRoot/unrestricted/${explicitDatePart(runDate)}")
    println(s"  isolated     -> $normalizedDailyRoot/isolated/${explicitDatePart(runDate)}")
    println(s"  click bots   -> $normalizedClickBotRoot/click_bot_uiids/${explicitDatePart(runDate)}")
  }

  private def readBidsImpressions(spark: SparkSession, root: String, env: String, runDate: LocalDate): DataFrame = {
    val normalizedRoot = root.stripSuffix("/")
    val path = s"$normalizedRoot/$env/bidsimpressions/${explicitDatePart(runDate)}"
    val bidColumns = Seq("IsImp", "BidRequestId", "UIID", "AdGroupId", "CampaignId", "AdvertiserId", "LogEntryTime")
    spark.read.parquet(path).select(bidColumns.map(c => col(c)): _*)
  }

  private def readClicks(spark: SparkSession, root: String, runDate: LocalDate): DataFrame = {
    val normalizedRoot = root.stripSuffix("/")
    val yyyymmdd = f"${runDate.getYear}%04d${runDate.getMonthValue}%02d${runDate.getDayOfMonth}%02d"
    val path = s"$normalizedRoot/date=$yyyymmdd/hour=*"
    spark.read.parquet(path).select(col("BidRequestId").cast("string").alias("BidRequestId"))
  }

  private def readIsolatedAdvertisers(spark: SparkSession, csvPath: String): DataFrame = {
    val withHeader = spark.read.option("header", "true").csv(csvPath)
    val df = if (withHeader.columns.contains("AdvertiserId")) {
      withHeader
    } else {
      spark.read.option("header", "false").csv(csvPath).withColumnRenamed("_c0", "AdvertiserId")
    }
    df.select("AdvertiserId").dropDuplicates("AdvertiserId").na.drop(Seq("AdvertiserId"))
  }

  private def writeDataset(
      df: DataFrame,
      root: String,
      subset: String,
      runDate: LocalDate,
      partitions: Int
  ): Unit = {
    val normalizedRoot = root.stripSuffix("/")
    val path = s"$normalizedRoot/$subset/${explicitDatePart(runDate)}"
    df.repartition(partitions).write.mode("overwrite").parquet(path)
  }

  private def parseIntList(raw: String): Seq[Int] = {
    Option(raw)
      .map(_.split(",").toSeq.flatMap(entry => Option(entry.trim).filter(_.nonEmpty).map(_.toInt)))
      .filter(_.nonEmpty)
      .getOrElse(DefaultRoiGoalTypes)
  }

  

  private def prodTestMapping(env: String): (String, String) =
    if (env == "prodTest") ("prod", "test") else (env, env)
}
