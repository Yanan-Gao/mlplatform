package com.thetradedesk.audience.jobs

import com.thetradedesk.audience.{date, dateFormatter, ttdEnv}
import com.thetradedesk.spark.TTDSparkContext.spark
import com.thetradedesk.spark.TTDSparkContext.spark.implicits._
import com.thetradedesk.confetti.AutoConfigResolvingETLJobBase
import com.thetradedesk.spark.util.TTDConfig.config
import com.thetradedesk.spark.util.prometheus.PrometheusClient
import org.apache.spark.sql.functions._


import java.time.LocalDateTime

case class TdidSeedScoreScaleConfig(
  salt: String,
  raw_score_path: String,
  seed_id_path: String,
  policy_table_path: String,
  out_path: String,
  population_score_path: String,
  smooth_factor: Double,
  userLevelUpperCap: Double,
  accuracy: Int,
  sampleRateForPercentile: Double,
  skipPercentile: Boolean,
  date_time: String
)

object TdidSeedScoreScale
  extends AutoConfigResolvingETLJobBase[TdidSeedScoreScaleConfig](
    groupName = "audience",
    jobName = "TdidSeedScoreScale") {

  override val prometheus: Option[PrometheusClient] = None

  /////
  override def runETLPipeline(): Map[String, String] = {
    val conf = getConfig
    val dt = LocalDateTime.parse(conf.date_time)
    date = dt.toLocalDate
    val dateStr = date.format(dateFormatter)
    val salt = conf.salt
    val raw_score_path = conf.raw_score_path
    val seed_id_path = conf.seed_id_path
    val policy_table_path = conf.policy_table_path
    val out_path = conf.out_path
    val population_score_path = conf.population_score_path
    val smooth_factor = conf.smooth_factor.toFloat
    val userLevelUpperCap = conf.userLevelUpperCap.toFloat
    val accuracy = conf.accuracy
    val sampleRateForPercentile = conf.sampleRateForPercentile
    val skipPercentile = conf.skipPercentile
    val df_seed_list = spark.read.format("parquet").load(seed_id_path)
    val seed_info = df_seed_list.collect()
    //val arrayLength = df_seed_list.select("SeedId").as[Seq[String]].first().length
    val minScore = seed_info(0).getAs[Seq[Float]]("MinScore")
    val maxScore = seed_info(0).getAs[Seq[Float]]("MaxScore")
    val avgScore = seed_info(0).getAs[Seq[Float]]("PopulationRelevance")
    val locationFactors = seed_info(0).getAs[Seq[Float]]("LocationFactor")
    val baselineHitRate = seed_info(0).getAs[Seq[Float]]("BaselineHitRate")

    val arrayLength = minScore.length
    def scoreScale_(rawScore: Array[Float], maxScore: Array[Float], minScore: Array[Float],
                    locationFactors:Array[Float], baselineHitRate:Array[Float]):Array[Float] = {
      val ret = new Array[Float](arrayLength)
      for (i <- 0 until arrayLength) {
        val scaled = math.min(math.max(rawScore(i)-minScore(i),0.0f)/(maxScore(i)-minScore(i)),1.0f )
        val weight = baselineHitRate(i) + (1 - baselineHitRate(i)) * (1 - 1 / (1 + math.exp(-(-smooth_factor * (scaled - locationFactors(i))))))

        ret(i) = (weight * scaled).floatValue()
      }
      ret
    }

    val scoreScale = udf(scoreScale_ _)

    // we need to apply the normalization for the population score as well
    // otherwise the final avg (score) won't be near 1
    val df_raw_score = spark.read.format("parquet").load(raw_score_path)

    val df_normalized = df_raw_score
      .withColumn("min_array", lit(minScore))
      .withColumn("max_array", lit(maxScore))
      .withColumn("locationFactors", lit(locationFactors))
      .withColumn("baselineHitRate", lit(baselineHitRate))
      .withColumn("Score", scoreScale('Score, 'max_array, 'min_array, 'locationFactors, 'baselineHitRate))
      .drop("max_array", "min_array")
      .cache()


    val df_final = df_normalized
      .withColumn("avg_array", lit(avgScore))
      .withColumn("Score", expr(s"transform(Score, (x, i) -> cast(least(${userLevelUpperCap}, x / avg_array[i]) as float))"))
      .drop("avg_array")

    df_final
      .withColumn("split", abs(xxhash64(concat(col("TDID"), lit(salt)))) % lit(10))
      .write
      .partitionBy("split")
      .format("parquet")
      .option("codec", "org.apache.hadoop.io.compress.GzipCodec")
      .mode("overwrite")
      .save(out_path)

    // PopulationScore

    // p25, p50, p75
    val tiles = skipPercentile match {
      case true => spark.range(1, 2).toDF("pos").withColumn("p25", lit(0.0f)).withColumn("p50", lit(0.0f)).withColumn("p75", lit(0.0f))
      case false => df_normalized
        .withColumn("rnd", rand())
        .filter(s"rnd < ${sampleRateForPercentile}") // not necessary to process every row for the percentile, to speed up
        .selectExpr("posexplode(Score) as (pos, score)")
        .groupBy('pos)
        .agg(percentile_approx('score, lit(Array(0.25, 0.5, 0.75)), lit(accuracy)).alias("percentile"))
        .withColumn("p25", element_at(col("percentile"), 1))
        .withColumn("p50", element_at(col("percentile"), 2))
        .withColumn("p75", element_at(col("percentile"), 3))
        .select("pos", "p25", "p50", "p75")
    }

    df_seed_list.selectExpr("posexplode(arrays_zip(SeedId, ActiveSize, PopulationRelevance)) as (pos, d)")
      .selectExpr("pos", "d.SeedId", "d.ActiveSize", "d.PopulationRelevance as PopulationSeedScoreRaw")
      .join(tiles, Seq("pos"), "left")
      .withColumn("PopulationSeedScore", lit(1.0))
      .select("SeedId", "PopulationSeedScore", "ActiveSize", "PopulationSeedScoreRaw", "p25", "p50", "p75") // keep the raw population score, in case need it for debug
      .coalesce(1)
      .write
      .format("parquet")
      .mode("overwrite")
      .save(population_score_path)

    Map("status" -> "success")
  }
}
