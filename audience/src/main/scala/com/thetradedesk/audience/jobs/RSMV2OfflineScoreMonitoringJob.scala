package com.thetradedesk.audience.jobs

import com.thetradedesk.audience.transform.IDTransform.IDType
import com.thetradedesk.audience.{audienceVersionDateFormat, dateFormatter, dateTime, ttdReadEnv}
import com.thetradedesk.featurestore.data.cbuffer.SchemaHelper.CBufferDataFrameReader
import com.thetradedesk.spark.util.TTDConfig.config
import com.thetradedesk.spark.TTDSparkContext.spark
import com.thetradedesk.spark.TTDSparkContext.spark.implicits._
import com.thetradedesk.spark.util.opentelemetry.OtelClient
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._

import java.time.format.DateTimeFormatter

object RSMV2OfflineScoreMonitoringJob {
  val prometheus = new OtelClient("RelevanceModelJob", "RSMV2OfflineScoreMonitoringJob")

  private val imp2BrModelInferencePath = config.getString("imp2BrModelInferencePath", s"s3://thetradedesk-mlplatform-us-east-1/data//${ttdReadEnv}/audience/RSMV2/Imp_Seed_None/v=2/${dateTime.format(DateTimeFormatter.ofPattern(audienceVersionDateFormat))}")
  private val tdid2SeedIdPath = config.getString("tdid2SeedIdPath", s"s3a://thetradedesk-mlplatform-us-east-1/data/${ttdReadEnv}/audience/scores/tdid2seedid/v=1/date=${dateTime.format(dateFormatter)}/split=0")

  def main(args: Array[String]): Unit = {
    val toIDTypeName = udf((i: Int) => IDType(i).toString)

    val brs = spark.read.cb(imp2BrModelInferencePath)
      .withColumn("IDType", toIDTypeName(col("IDType")))
      .select("TDID", "IDType").distinct()

    val tdidIdTypeToScoreArray = spark.read
      .parquet(tdid2SeedIdPath)
      .join(brs, Seq("TDID"))

    val tdidIdTypeToScore = tdidIdTypeToScoreArray
      .select($"*", posexplode($"Score").as(Seq("SeedPos", "SeedScore")))
      .drop("Score")

    monitorIdTypeScores(tdidIdTypeToScore)

    prometheus.pushMetrics()
  }

  private def monitorIdTypeScores(tdidIdTypeToScore: DataFrame): Unit = {
    val avgSeedIdTypeScore = tdidIdTypeToScore
      .groupBy("SeedPos", "IDType")
      .agg(avg("SeedScore").alias("avg_seed_score"))

    val avgSeedIdTypeScoreRelativeDifference = avgSeedIdTypeScore.as("a")
      .join(avgSeedIdTypeScore.as("b"), $"a.SeedPos" === $"b.SeedPos")
      .withColumn(
        "relative_diff",
        ($"a.avg_seed_score" - $"b.avg_seed_score") / $"b.avg_seed_score"
      )

    val medianIdTypeScoreRelativeDifference = avgSeedIdTypeScoreRelativeDifference
      .groupBy("a.IDType", "b.IDType")
      .agg(percentile_approx(col("relative_diff"), lit(0.5), lit(10000)).alias("median_relative_diff"))
      .collect()

    val medianIdTypeRelativeDiffGauge = prometheus.createGauge(
      "rsmv2_offline_score_monitoring_median_id_type_relative_diff",
      "Median relative difference of relevance scores between ID types"
    )

    medianIdTypeScoreRelativeDifference.foreach { row =>
      val idTypeA = row.getString(0)
      val idTypeB = row.getString(1)
      val medianRelativeDiff = row.getDouble(2)

      medianIdTypeRelativeDiffGauge
        .labels(
          Map(
            "id_type_a" -> idTypeA,
            "id_type_b" -> idTypeB
          )
        )
        .set(medianRelativeDiff)
    }
  }
}
