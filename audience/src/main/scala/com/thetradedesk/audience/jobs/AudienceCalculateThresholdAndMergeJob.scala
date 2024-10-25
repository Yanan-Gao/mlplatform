package com.thetradedesk.audience.jobs

import com.thetradedesk.spark.TTDSparkContext.spark
import com.thetradedesk.spark.util.TTDConfig.config
import com.thetradedesk.spark.util.prometheus.PrometheusClient
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.expressions.Window
import com.thetradedesk.spark.TTDSparkContext.spark.implicits._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.FloatType

object AudienceCalculateThresholdAndMergeJob {
  val prometheus = new PrometheusClient("AudienceModelJob", "AudienceCalculateThresholdAndMergeJob")

  object Config {
    val sourceDataFolder = config.getStringRequired("sourceDataFolder")
    val predictionResultsFolder = config.getStringRequired("predictionResultsFolder")
    val targetDataFolder = config.getStringRequired("targetDataFolder")
    val thresholdBeta = config.getDouble("thresholdBeta", 0.5)
    val writeMode = config.getString("writeMode", "error")
  }

  def main(args: Array[String]): Unit = {
    runETLPipeline()
    prometheus.pushMetrics()
  }

  def runETLPipeline(): Unit = {
    val predictionResults = spark.read.parquet(Config.predictionResultsFolder)
      .withColumnRenamed("SyntheticIds", "SyntheticId")
      .where('SyntheticId > 0)
    val sourceDf = spark.read.parquet(Config.sourceDataFolder)

    val start = BigDecimal(0.01)
    val end = BigDecimal(1.0)
    val step = BigDecimal(0.01)


    sourceDf.join(
        fScoreCalculate(predictionResults, start, end, step, beta = Config.thresholdBeta), Seq("SyntheticId"), "left"
      )
      .withColumn("Threshold", coalesce('Threshold, lit(0.5)).cast(FloatType)) // when threshold not existing, we can calculate relevance score for them use 0.5
      .withColumn("BestScore", coalesce('BestScore, lit(-1)).cast(FloatType)) // -1 means threshold is default value
      .write.mode(Config.writeMode).parquet(Config.targetDataFolder)
  }

  def fScoreCalculate(df: DataFrame, start: BigDecimal, end: BigDecimal, step: BigDecimal, beta: Double = 0.8f): DataFrame = {
    val thresholds: IndexedSeq[Double] = (start to end by step).map(_.toDouble)

    // Define a window for partitioning by 'SyntheticId' and sorting by 'pred'
    // val windowSpec = Window.partitionBy("SyntheticId").orderBy(desc("pred"))
    // List of thresholds to test

    val betaSquared = beta * beta
    // Loop over thresholds to calculate precision, recall, and F-beta score for each SyntheticId
    val results = thresholds.map { threshold =>
      // Generate a label based on the threshold (1 if pred >= threshold, else 0)
      val dfWithLabels = df.withColumn("prediction", when(col("pred") >= threshold, 1).otherwise(0))
      // Calculate true positives, false positives, and false negatives
      val dfWithStats = dfWithLabels.withColumn(
        "true_positive", when(col("prediction") === 1 && col("target") === 1, 1).otherwise(0)
      ).withColumn(
        "false_positive", when(col("prediction") === 1 && col("target") === 0, 1).otherwise(0)
      ).withColumn(
        "false_negative", when(col("prediction") === 0 && col("target") === 1, 1).otherwise(0)
      )
      // Aggregate by 'SyntheticId' to compute metrics
      val dfAggregated = dfWithStats.groupBy("SyntheticId").agg(
        sum("true_positive").alias("tp"),
        sum("false_positive").alias("fp"),
        sum("false_negative").alias("fn")
      )
      // Calculate precision, recall, and F-beta score
      val dfWithFbeta = dfAggregated.withColumn("precision", col("tp") / (col("tp") + col("fp")))
        .withColumn("recall", col("tp") / (col("tp") + col("fn")))
        .withColumn("BestScore",
          when((col("precision").isNull) || (col("recall").isNull) || ((col("precision") === 0) && (col("recall") === 0)), 0.0)
            .otherwise(
              lit((1 + betaSquared)) * (col("precision") * col("recall")) / (lit(betaSquared) * col("precision") + col("recall"))
            )
        ).withColumn("Threshold", lit(threshold))

      dfWithFbeta.select("SyntheticId", "Threshold", "BestScore")
    }
    // Union all results and find the threshold that maximizes the F-beta score for each 'SyntheticId'
    val allResults = results.reduce(_ union _)
    val windowSpec = Window.partitionBy("SyntheticId").orderBy(desc("BestScore"))

    val finalResults = allResults.withColumn("rank", row_number().over(windowSpec))
      .filter(col("rank") === 1)
      .select('SyntheticId, 'Threshold, 'BestScore)
    finalResults
  }
}
