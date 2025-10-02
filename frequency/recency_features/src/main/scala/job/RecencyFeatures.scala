package job

import java.time.LocalDate

import com.thetradedesk.geronimo.bidsimpression.schema.BidsImpressions
import com.thetradedesk.frequency.schema.{ClickTrackerDataSet, RecencyOutputDataSet, UnifiedAdGroupDataSet, CampaignROIGoalDataSet, AdvertiserExclusionList}
import com.thetradedesk.frequency.transform.RecencyFeaturesTransform
import com.thetradedesk.spark.TTDSparkContext.spark
import com.thetradedesk.spark.util.TTDConfig.config
import org.apache.spark.sql.{DataFrame, SparkSession}
import com.thetradedesk.geronimo.shared.explicitDatePart
import org.apache.spark.sql.functions._

object RecencyFeatures {

  private val DefaultEnv = "prodTest"
  private val DefaultLookback = 3
  private val DefaultPartitions = 200
  private val DefaultRoiGoalTypes = Seq(3, 4)
  private val DefaultIsolatedCsv = AdvertiserExclusionList.ADVERTISEREXCLUSIONS3

  def main(args: Array[String]): Unit = {
    val runDate = config.getDate("date", LocalDate.now())
    val env = config.getString("ttd.env", DefaultEnv)
    val (readEnv, writeEnv) = if (env == "prodTest") ("prod", "test") else (env, env)

    val lookbackDays = config.getInt("lookbackDays", DefaultLookback)
    val partitions = config.getInt("partitions", DefaultPartitions)
    val roiGoalTypes = parseIntList(config.getString("roiGoalTypes", DefaultRoiGoalTypes.mkString(",")), DefaultRoiGoalTypes)

    val isolatedCsv = Option(config.getString("isolatedCsv", AdvertiserExclusionList.ADVERTISEREXCLUSIONS3)).filter(_.nonEmpty)
    val uiidBotFilteringRoot = Option(config.getString("uiidBotFilteringRoot", "")).filter(_.nonEmpty)
    val outputRoot = config.getString("outputRoot", RecencyOutputDataSet.root(writeEnv))
    val koaEnv = Option(config.getString("koaEnvOverwrite", readEnv))
      .map(_.trim)
      .filter(_.nonEmpty)
      .getOrElse(readEnv)

    val bidsImpressionsRoot = config.getString("bidsImpressionsRoot", BidsImpressions.BIDSIMPRESSIONSS3)
    val clickTrackerRoot = config.getString("clickTrackerRoot", ClickTrackerDataSet.CLICKSS3)

    val (bidsAll, clicksAll) = readHistoryRange(spark, bidsImpressionsRoot, clickTrackerRoot, koaEnv, runDate, lookbackDays)

    val roiKeys = buildRoiAdGroupKeys(runDate, roiGoalTypes)
    val isolated = isolatedCsv.map(readIsolatedAdvertisers(spark, _))
    val uiidBotFiltering = uiidBotFilteringRoot.map(root => {
      val clickBotRoot = if (root.contains("{env}")) root.replace("{env}", readEnv) else root
      readFrequencyDataset(spark, clickBotRoot, runDate, "click_bot_uiids")
    })

    val base = RecencyFeaturesTransform.prepareBase(bidsAll, clicksAll, roiKeys, uiidBotFiltering, isolated)
    val features = RecencyFeaturesTransform.addRecencyFeatures(base)

    val outputDf = features
      .where(col("date") === lit(java.sql.Date.valueOf(runDate)))
      .select(
        "BidRequestId",
        "UIID",
        "CampaignId",
        "AdvertiserId",
        "impression_time_unix",
        "date",
        "label",
        "uiid_campaign_recency_impression_seconds",
        "uiid_campaign_recency_click_seconds",
        "uiid_recency_impression_seconds",
        "uiid_recency_click_seconds"
      )

    writeOutput(outputDf, outputRoot, runDate, partitions)

    val normalizedOutputRoot = outputRoot.stripSuffix("/")
    println(s"RecencyFeatures completed for date $runDate")
    println(s"Output path: $normalizedOutputRoot/${explicitDatePart(runDate)}")
  }

  private def buildRoiAdGroupKeys(runDate: LocalDate, roiGoalTypes: Seq[Int]): DataFrame = {
    val adLatest = UnifiedAdGroupDataSet().readLatestPartitionUpTo(runDate, isInclusive = true)
    val croi = CampaignROIGoalDataSet().readLatestPartitionUpTo(runDate, isInclusive = true).where(col("Priority") === 1)
    val goalSet = roiGoalTypes.toSet
    require(goalSet.nonEmpty, "roiGoalTypes must contain at least one entry")
    adLatest.alias("ag")
      .join(croi.alias("cg"), Seq("CampaignId"), "left")
      .withColumn("goal", coalesce(col("cg.ROIGoalTypeId").cast("int"), col("ag.ROIGoalTypeId").cast("int")))
      .where(col("goal").isin(goalSet.toSeq.map(_.asInstanceOf[AnyRef]): _*))
      .select(col("ag.AdGroupId").alias("ROI_AdGroupId"), col("ag.CampaignId").alias("ROI_CampaignId"))
      .dropDuplicates("ROI_AdGroupId", "ROI_CampaignId")
  }

  private def readHistoryRange(
      spark: SparkSession,
      biRoot: String,
      clickRoot: String,
      env: String,
      runDate: LocalDate,
      lookbackDays: Int
  ): (DataFrame, DataFrame) = {
    require(lookbackDays >= 0, "lookbackDays must be non-negative")
    val dates = (0 to lookbackDays).map(delta => runDate.minusDays(delta.toLong))
    val bids = dates.map(d => readBidsImpressions(spark, biRoot, env, d)).reduce(_.unionByName(_, true))
    val clicks = dates.map(d => readClicks(spark, clickRoot, d)).reduce(_.unionByName(_, true))
    (bids, clicks)
  }

  private def readBidsImpressions(spark: SparkSession, root: String, env: String, date: LocalDate): DataFrame = {
    val normalizedRoot = root.stripSuffix("/")
    val path = s"$normalizedRoot/$env/bidsimpressions/${explicitDatePart(date)}"
    spark.read.parquet(path)
  }

  private def readClicks(spark: SparkSession, root: String, date: LocalDate): DataFrame = {
    val normalizedRoot = root.stripSuffix("/")
    val yyyymmdd = f"${date.getYear}%04d${date.getMonthValue}%02d${date.getDayOfMonth}%02d"
    val path = s"$normalizedRoot/date=$yyyymmdd/hour=*"
    spark.read.parquet(path).select(col("BidRequestId").cast("string").alias("BidRequestId"))
  }

  private def readIsolatedAdvertisers(spark: SparkSession, csvPath: String): DataFrame = {
    val withHeader = spark.read.option("header", "true").csv(csvPath)
    val df = if (withHeader.columns.contains("AdvertiserId")) withHeader
    else spark.read.option("header", "false").csv(csvPath).withColumnRenamed("_c0", "AdvertiserId")
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

  private def writeOutput(df: DataFrame, root: String, runDate: LocalDate, partitions: Int): Unit = {
    val normalizedRoot = root.stripSuffix("/")
    val path = s"$normalizedRoot/${explicitDatePart(runDate)}"
    df.repartition(partitions).write.mode("overwrite").parquet(path)
  }

  private def parseIntList(raw: String, default: Seq[Int]): Seq[Int] = {
    Option(raw)
      .map(_.split(",").toSeq.flatMap(v => Option(v.trim).filter(_.nonEmpty).map(_.toInt)))
      .filter(_.nonEmpty)
      .getOrElse(default)
  }

  
}
