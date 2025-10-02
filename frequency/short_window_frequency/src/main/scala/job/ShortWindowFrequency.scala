package job

import java.time.LocalDate

import com.thetradedesk.geronimo.bidsimpression.schema.BidsImpressions
import com.thetradedesk.frequency.schema.{ClickTrackerDataSet, ClickBotUiidsDataSet, ShortWindowOutputDataSet, AdvertiserExclusionList}
import com.thetradedesk.frequency.transform.ShortWindowFrequencyTransform
import com.thetradedesk.spark.TTDSparkContext.spark
import com.thetradedesk.spark.util.TTDConfig.config
import org.apache.spark.sql.{DataFrame, SparkSession}
import com.thetradedesk.geronimo.shared.explicitDatePart
import org.apache.spark.sql.functions._

object ShortWindowFrequency {

  private val DefaultEnv = "prodTest"
  private val DefaultPartitions = 200
  private val DefaultBlackout = -180
  private val DefaultRoiGoalTypes = Seq(3, 4)
  private val DefaultShortWindows = Seq(3600, 10800, 21600, 43200)
  private val DefaultIsolatedCsv = AdvertiserExclusionList.ADVERTISEREXCLUSIONS3
  def main(args: Array[String]): Unit = {
    val runDate = config.getDate("date", LocalDate.now())
    val env = config.getString("ttd.env", DefaultEnv)
    val (readEnv, writeEnv) = prodTestMapping(env)

    val partitions = config.getInt("partitions", DefaultPartitions)
    val blackout = config.getInt("blackout", DefaultBlackout)
    val roiGoalTypes = parseIntList(config.getString("roiGoalTypes", DefaultRoiGoalTypes.mkString(",")), DefaultRoiGoalTypes)
    val shortWindows = parseIntList(config.getString("shortWindows", DefaultShortWindows.mkString(",")), DefaultShortWindows)
    val outputRoot = config.getString("outputRoot", ShortWindowOutputDataSet.root(writeEnv))
    val isolatedCsv = Option(config.getString("isolatedCsv", AdvertiserExclusionList.ADVERTISEREXCLUSIONS3)).filter(_.nonEmpty)
    val uiidBotRoot = Option(config.getString("uiidBotFilteringRoot", ClickBotUiidsDataSet.root(readEnv))).filter(_.nonEmpty)
    val koaEnv = Option(config.getString("koaEnvOverwrite", readEnv))
      .map(_.trim)
      .filter(_.nonEmpty)
      .getOrElse(readEnv)

    val bidsImpressionsRoot = config.getString("bidsImpressionsRoot", BidsImpressions.BIDSIMPRESSIONSS3.stripSuffix("/"))
    val clickTrackerRoot = config.getString("clickTrackerRoot", ClickTrackerDataSet.CLICKSS3.stripSuffix("/"))

    val bidsToday = readBidsImpressions(spark, bidsImpressionsRoot, koaEnv, runDate)
    val bidsPrev = readBidsImpressions(spark, bidsImpressionsRoot, koaEnv, runDate.minusDays(1))
    val clicksToday = readClicks(spark, clickTrackerRoot, runDate)
    val clicksPrev = readClicks(spark, clickTrackerRoot, runDate.minusDays(1))

    val roiKeys = ShortWindowFrequencyTransform.buildRoiAdGroupKeys(
      spark,
      runDate,
      roiGoalTypes
    )

    val isolated = isolatedCsv.map(readIsolatedAdvertisers(spark, _))
    val uiidBotFiltering = uiidBotRoot.map(readFrequencyDataset(spark, _, runDate, "click_bot_uiids"))

    val base = ShortWindowFrequencyTransform.prepareBase(
      bidsToday,
      bidsPrev,
      clicksToday,
      clicksPrev,
      roiKeys,
      uiidBotFiltering,
      isolated
    )

    val features = ShortWindowFrequencyTransform.calculateShortWindowFeatures(
      base,
      runDate,
      shortWindows,
      blackout
    )

    writeShortWindowFeatures(features, outputRoot, runDate, partitions)

    val normalizedOutputRoot = outputRoot.stripSuffix("/")
    println(s"ShortWindowFrequency completed for date $runDate")
    println(s"Output written to $normalizedOutputRoot/${explicitDatePart(runDate)}")
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

  private def readFrequencyDataset(
      spark: SparkSession,
      root: String,
      runDate: LocalDate,
      subset: String
  ): DataFrame = {
    val normalizedRoot = root.stripSuffix("/")
    val path = s"$normalizedRoot/$subset/${explicitDatePart(runDate)}"
    spark.read.parquet(path)
  }

  private def writeShortWindowFeatures(
      df: DataFrame,
      root: String,
      runDate: LocalDate,
      partitions: Int
  ): Unit = {
    val normalizedRoot = root.stripSuffix("/")
    val path = s"$normalizedRoot/${explicitDatePart(runDate)}"
    df.repartition(partitions).write.mode("overwrite").parquet(path)
  }

  private def parseIntList(raw: String, default: Seq[Int]): Seq[Int] = {
    Option(raw)
      .map(_.split(",").toSeq.flatMap(value => Option(value.trim).filter(_.nonEmpty).map(_.toInt)))
      .filter(_.nonEmpty)
      .getOrElse(default)
  }

  

  private def prodTestMapping(env: String): (String, String) =
    if (env == "prodTest") ("prod", "test") else (env, env)
}
