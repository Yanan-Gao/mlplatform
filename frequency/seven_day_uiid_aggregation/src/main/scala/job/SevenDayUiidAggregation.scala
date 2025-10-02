package job

import java.time.LocalDate

import com.thetradedesk.frequency.schema.{DailyAggregateDataSet, SevenDayAggregationDataSet}
import com.thetradedesk.frequency.transform.SevenDayUiidAggregationTransform
import com.thetradedesk.spark.TTDSparkContext.spark
import com.thetradedesk.spark.util.TTDConfig.config
import org.apache.spark.sql.DataFrame
import com.thetradedesk.geronimo.shared.explicitDatePart

object SevenDayUiidAggregation {

  private val DefaultEnv = "prodTest"
  private val DefaultSubset = "unrestricted"
  private val DefaultLookbackDays = 7
  private val DefaultPartitions = 200

  def main(args: Array[String]): Unit = {
    val runDate = config.getDate("date", LocalDate.now())
    val env = config.getString("ttd.env", DefaultEnv)
    val (readEnv, writeEnv) = prodTestMapping(env)

    val subset = config.getString("subset", DefaultSubset)
    val lookbackDays = config.getInt("lookbackDays", DefaultLookbackDays)
    val partitions = config.getInt("partitions", DefaultPartitions)
    val dailyRoot = config.getString("dailyRoot", DailyAggregateDataSet.root(readEnv))
    val outputRoot = config.getString("outputRoot", SevenDayAggregationDataSet.root(writeEnv))
    val aggregateSets = {
      val raw = config.getString("aggregateSets", "")
      val parsed = parseAggregateSets(raw)
      if (parsed.nonEmpty) parsed else DefaultAggregateSets
    }

    // For visibility: default lookback and sets
    val _ = DefaultAggregateSets

    val (perUiidCampaign, perUiid, loadedOffsets) = SevenDayUiidAggregationTransform.buildSevenDayViews(
      spark,
      readEnv,
      subset,
      runDate,
      lookbackDays,
      dailyRoot,
      aggregateSets
    )

    val partitionsUiid = math.max(partitions / 5, 1)
    writeOutput(perUiidCampaign, outputRoot, subset, "uiid_campaign", runDate, partitions)
    writeOutput(perUiid, outputRoot, subset, "per_uiid", runDate, partitionsUiid)

    val normalizedOutputRoot = outputRoot.stripSuffix("/")
    val offsetsStr = loadedOffsets.sorted.mkString(", ")
    println(s"SevenDayUiidAggregation completed for date $runDate")
    println(s"Loaded offsets (days back): $offsetsStr")
    println(s"UIID+Campaign output -> $normalizedOutputRoot/$subset/uiid_campaign/${explicitDatePart(runDate)}")
    println(s"Per-UIID output    -> $normalizedOutputRoot/$subset/per_uiid/${explicitDatePart(runDate)}")

    if (aggregateSets.nonEmpty) {
      val setsStr = aggregateSets.map(_.mkString("[", ", ", "]")).mkString(", ")
      println(s"Aggregate sets applied: $setsStr")
    }
  }

  private def writeOutput(
      df: DataFrame,
      root: String,
      subset: String,
      category: String,
      runDate: LocalDate,
      partitions: Int
  ): Unit = {
    val normalizedRoot = root.stripSuffix("/")
    val path = s"$normalizedRoot/$subset/$category/${explicitDatePart(runDate)}"
    df.repartition(partitions).write.mode("overwrite").parquet(path)
  }

  private def parseAggregateSets(raw: String): Seq[Seq[Int]] = {
    val cleaned = Option(raw).map(_.trim).getOrElse("")
    if (cleaned.isEmpty) {
      Seq.empty
    } else {
      import com.fasterxml.jackson.databind.{JsonNode, ObjectMapper}
      import scala.collection.JavaConverters._

      val mapper = new ObjectMapper()
      val root: JsonNode = try mapper.readTree(cleaned) catch {
        case e: Exception =>
          throw new IllegalArgumentException(s"aggregate_sets must be valid JSON: ${e.getMessage}")
      }

      if (!root.isArray) throw new IllegalArgumentException("aggregate_sets must parse to a list of lists")

      root.elements().asScala.zipWithIndex.flatMap { case (entry, idx) =>
        if (!entry.isArray) throw new IllegalArgumentException(s"aggregate_sets entry #${idx + 1} is not a list")
        val ints = entry.elements().asScala.map { n =>
          if (!n.isNumber) throw new IllegalArgumentException(s"aggregate_sets entry #${idx + 1} contains non-integer offset")
          val v = n.numberValue().intValue()
          if (v <= 0) throw new IllegalArgumentException("aggregate_sets offsets must be positive integers")
          v
        }.toSeq
        val deduped = ints.foldLeft((Seq.empty[Int], Set.empty[Int])) { case ((acc, seen), v) =>
          if (seen(v)) (acc, seen) else (acc :+ v, seen + v)
        }._1
        if (deduped.nonEmpty) Some(deduped) else None
      }.toSeq
    }
  }

  private val DefaultAggregateSets: Seq[Seq[Int]] = Seq(
    Seq(1, 2),
    Seq(1, 2, 3, 4),
    Seq(1, 2, 3, 4, 5, 6)
  )

  

  private def prodTestMapping(env: String): (String, String) =
    if (env == "prodTest") ("prod", "test") else (env, env)
}
