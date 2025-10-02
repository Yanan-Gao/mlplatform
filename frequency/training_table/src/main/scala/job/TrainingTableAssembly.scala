package job

import java.time.LocalDate

import com.thetradedesk.frequency.schema.{ShortWindowDataSet, SevenDayAggregationDataSet, RecencyOutputDataSet, TrainingTableDataSet}
import com.thetradedesk.frequency.transform.TrainingTableTransform
import com.thetradedesk.spark.TTDSparkContext.spark
import com.thetradedesk.spark.util.TTDConfig.config
import org.apache.spark.sql.{DataFrame, SparkSession}
import com.thetradedesk.geronimo.shared.explicitDatePart
import org.apache.spark.sql.functions._

object TrainingTableAssembly {

  private val DefaultEnv = "prodTest"
  private val DefaultPartitions = 200
  private val DefaultAggregateSets: Seq[Seq[Int]] = Seq(Seq(1, 2), Seq(1, 2, 3, 4), Seq(1, 2, 3, 4, 5, 6))

  def main(args: Array[String]): Unit = {
    val runDate = config.getDate("date", LocalDate.now())
    val env = config.getString("ttd.env", DefaultEnv)
    val (readEnv, writeEnv) = if (env == "prodTest") ("prod", "test") else (env, env)

    val subset = config.getString("subset", "unrestricted")
    val partitions = config.getInt("partitions", DefaultPartitions)
    val shortWindowRoot = config.getString("shortWindowRoot", ShortWindowDataSet.root(readEnv))
    val sevenDayRoot = config.getString("sevenDayRoot", SevenDayAggregationDataSet.root(readEnv))
    val recencyRoot = config.getString("recencyRoot", RecencyOutputDataSet.root(readEnv))
    val outputRoot = config.getString("outputRoot", TrainingTableDataSet.root(writeEnv))

    val aggregateSets = parseAggregateSets(config.getString("aggregateSets", "")) match {
      case Nil => DefaultAggregateSets
      case xs => xs
    }

    val shortWindow = readShortWindow(spark, shortWindowRoot, runDate)
    val recency = readRecency(spark, recencyRoot, runDate)
      .select(
        "BidRequestId",
        "uiid_campaign_recency_impression_seconds",
        "uiid_campaign_recency_click_seconds",
        "uiid_recency_impression_seconds",
        "uiid_recency_click_seconds"
      )
    val sevenDayCampaign = readSevenDayUiidCampaign(spark, sevenDayRoot, subset, runDate)
    val sevenDayUiid = readSevenDayUiid(spark, sevenDayRoot, subset, runDate)

    val mainDf = shortWindow.alias("sw")
      .join(recency.alias("rc"), Seq("BidRequestId"), "left")
      .join(sevenDayCampaign.alias("sdc"), Seq("UIID", "CampaignId", "AdvertiserId"), "left")
      .join(sevenDayUiid.alias("sdu"), Seq("UIID"), "left")

    val hourFraction = (
      hour(from_unixtime(col("sw.impression_time_unix"))).cast("double") +
        minute(from_unixtime(col("sw.impression_time_unix"))) / lit(60.0)
    ) / lit(24.0)

    val mainDfWithProgress = mainDf.withColumn("day_progress_fraction", hourFraction)

    val sameDayImpsCol = "campaign_impression_count_same_day"
    val sameDayClicksCol = "campaign_click_count_same_day"
    val sameDayImpsUiidCol = "user_impression_count_same_day"
    val sameDayClicksUiidCol = "user_click_count_same_day"

    val lookup = TrainingTableTransform.buildOffsetLookup(aggregateSets)
    require(lookup.nonEmpty, "aggregate_sets must contain at least one entry to compute long windows")

    val withCampaignLong = TrainingTableTransform.computeLongWindowApprox(
      mainDfWithProgress,
      lookup,
      "day_progress_fraction",
      sameDayImpsCol,
      sameDayClicksCol,
      "impression_count_offsets_",
      "click_count_offsets_",
      "impression_count_d",
      "click_count_d",
      "same_campaign_imps_",
      "same_campaign_clicks_"
    )

    val withUiidLong = TrainingTableTransform.computeLongWindowApprox(
      withCampaignLong,
      lookup,
      "day_progress_fraction",
      sameDayImpsUiidCol,
      sameDayClicksUiidCol,
      "uiid_impression_count_offsets_",
      "uiid_click_count_offsets_",
      "uiid_impression_count_d",
      "uiid_click_count_d",
      "cross_campaign_imps_",
      "cross_campaign_clicks_"
    )

    val featureCols = collection.mutable.ArrayBuffer[String]()
    featureCols ++= Seq("BidRequestId", "UIID", "CampaignId", "AdvertiserId", "impression_time_unix", "date", "label")
    featureCols ++= withUiidLong.columns.filter(c => c.startsWith("campaign_") || c.startsWith("user_"))
    featureCols ++= withUiidLong.columns.filter(_.startsWith("same_campaign_"))
    featureCols ++= withUiidLong.columns.filter(_.startsWith("cross_campaign_"))
    featureCols ++= withUiidLong.columns.filter(_.startsWith("uiid_"))
    featureCols ++= withUiidLong.columns.filter(_.startsWith("day_progress"))

    val outputDf = withUiidLong.select(featureCols.distinct.map(col): _*)

    val schema = outputDf.schema
    val numericTypes = Set("bigint", "int", "double", "float")
    val countCols = schema.fields.collect {
      case field if (field.name.contains("impression_count") || field.name.contains("click_count") || field.name.contains("imps") || field.name.contains("clicks")) && numericTypes.contains(field.dataType.simpleString) =>
        field.name
    }
    val filledCounts = if (countCols.nonEmpty) outputDf.na.fill(0, countCols.toArray) else outputDf

    val recencyCols = Seq(
      "uiid_campaign_recency_impression_seconds",
      "uiid_campaign_recency_click_seconds",
      "uiid_recency_impression_seconds",
      "uiid_recency_click_seconds"
    )
    val presentRecencyCols = recencyCols.filter(filledCounts.columns.contains)
    val fillValueRecency = 6 * 24 * 3600
    val finalDf = if (presentRecencyCols.nonEmpty) filledCounts.na.fill(fillValueRecency, presentRecencyCols.toArray) else filledCounts

    writeOutput(finalDf, outputRoot, subset, runDate, partitions)

    val normalizedOutputRoot = outputRoot.stripSuffix("/")
    val offsetsStr = aggregateSets.map(_.sorted.mkString("[", ", ", "]")).mkString(", ")
    println(s"TrainingTable assembled with aggregate sets: $offsetsStr")
    println(s"Output path: $normalizedOutputRoot/$subset/${explicitDatePart(runDate)}")
  }

  private def readShortWindow(spark: SparkSession, root: String, date: LocalDate): DataFrame = {
    val normalizedRoot = root.stripSuffix("/")
    val path = s"$normalizedRoot/${explicitDatePart(date)}"
    spark.read.parquet(path)
  }

  private def readSevenDayUiidCampaign(spark: SparkSession, root: String, subset: String, date: LocalDate): DataFrame = {
    val normalizedRoot = root.stripSuffix("/")
    val path = s"$normalizedRoot/$subset/uiid_campaign/${explicitDatePart(date)}"
    spark.read.parquet(path)
  }

  private def readSevenDayUiid(spark: SparkSession, root: String, subset: String, date: LocalDate): DataFrame = {
    val normalizedRoot = root.stripSuffix("/")
    val path = s"$normalizedRoot/$subset/per_uiid/${explicitDatePart(date)}"
    spark.read.parquet(path)
  }

  private def readRecency(spark: SparkSession, root: String, date: LocalDate): DataFrame = {
    val normalizedRoot = root.stripSuffix("/")
    val path = s"$normalizedRoot/${explicitDatePart(date)}"
    spark.read.parquet(path)
  }

  private def writeOutput(df: DataFrame, root: String, subset: String, date: LocalDate, partitions: Int): Unit = {
    val normalizedRoot = root.stripSuffix("/")
    val path = s"$normalizedRoot/$subset/${explicitDatePart(date)}"
    df.repartition(partitions).write.mode("overwrite").parquet(path)
  }

  private def parseAggregateSets(raw: String): Seq[Seq[Int]] = {
    val cleaned = Option(raw).map(_.trim).getOrElse("")
    if (cleaned.isEmpty) Nil
    else {
      import com.fasterxml.jackson.databind.{JsonNode, ObjectMapper}
      import scala.collection.JavaConverters._

      val mapper = new ObjectMapper()
      val root: JsonNode = try mapper.readTree(cleaned) catch {
        case e: Exception =>
          throw new IllegalArgumentException(s"aggregate_sets must be valid JSON: ${e.getMessage}")
      }

      if (!root.isArray) throw new IllegalArgumentException("aggregate_sets must parse to a list of lists")

      root.elements().asScala.zipWithIndex.map { case (entry, idx) =>
        if (!entry.isArray) throw new IllegalArgumentException(s"aggregate_sets entry #${idx + 1} is not a list")
        val ints = entry.elements().asScala.map { n =>
          if (!n.isNumber) throw new IllegalArgumentException(s"aggregate_sets entry #${idx + 1} contains non-integer offset")
          val v = n.numberValue().intValue()
          if (v <= 0) throw new IllegalArgumentException("aggregate_sets offsets must be positive integers")
          v
        }.toSeq
        ints.foldLeft((Seq.empty[Int], Set.empty[Int])) { case ((acc, seen), v) => if (seen(v)) (acc, seen) else (acc :+ v, seen + v) }._1
      }.toSeq
    }
  }

  
}
