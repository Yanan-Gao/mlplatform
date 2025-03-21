package com.thetradedesk.audience.jobs

import com.thetradedesk.audience.datasets.{CrossDeviceVendor, DataSource}
import com.thetradedesk.audience.{date, dateFormatter, ttdEnv}
import com.thetradedesk.spark.TTDSparkContext.spark
import com.thetradedesk.spark.TTDSparkContext.spark.implicits._
import com.thetradedesk.spark.util.TTDConfig.config
import org.apache.spark.sql.functions._


object TdidSeedScoreScale {
  val salt=config.getString("salt", "TRM")

  val dateStr = date.format(dateFormatter)
  val raw_score_path = config.getString("raw_score_path", s"s3://thetradedesk-mlplatform-us-east-1/data/${ttdEnv}/audience/scores/tdid2seedid_raw/v=1/date=${dateStr}/")
  val seed_id_path = config.getString(
    "seed_id_path", s"s3://thetradedesk-mlplatform-us-east-1/data/${ttdEnv}/audience/scores/seedids/v=2/date=${dateStr}/")
  val policy_table_path = config.getString(
    "policy_table_path", s"s3://thetradedesk-mlplatform-us-east-1/configdata/prod/audience/policyTable/RSM/v=1/${dateStr}000000/")

  val out_path = config.getString("out_path", s"s3://thetradedesk-mlplatform-us-east-1/data/${ttdEnv}/audience/scores/tdid2seedid/v=1/date=${dateStr}/")
  val population_score_path = config.getString("population_score_path", s"s3://thetradedesk-mlplatform-us-east-1/data/${ttdEnv}/audience/scores/seedpopulationscore/v=1/date=${dateStr}/")

  val r = config.getDouble("r", 1e-8f).toFloat
  val smooth_factor = config.getDouble("smooth_factor", 30f).toFloat
  val loc_factor = config.getDouble("loc_factor", 0.8f).toFloat
  val userLevelUpperCap = config.getDouble("userLevelUpperCap", 1e6f).toFloat

  /////
  def runETLPipeline(): Unit = {
    val df_seed_list = spark.read.format("parquet").load(seed_id_path)
    val arrayLength = df_seed_list.select("SeedId").as[Seq[String]].first().length
    val elementWiseSumUdf = udaf(new ElementWiseAggregator(arrayLength, (a, b) => a + b, 0.0f))
    val elementWiseMinUdf = udaf(new ElementWiseAggregator(arrayLength, math.min, Float.MaxValue))
    val elementWiseMaxUdf = udaf(new ElementWiseAggregator(arrayLength, math.max, Float.MinValue))

    def scoreScale_(rawScore: Array[Float], maxScore: Array[Float], minScore: Array[Float]):Array[Float] = {
      val ret = new Array[Float](arrayLength)
      for (i <- 0 until arrayLength) {
        val scaled = math.min(math.max(rawScore(i)-minScore(i),0.0f)/(maxScore(i)-minScore(i)),1.0f )
        val weight = r + (1 - r) * (1 - 1 / (1 + math.exp(-(-smooth_factor * (scaled - loc_factor)))))

        ret(i) = (weight * scaled).floatValue()
      }
      ret
    }

    val scoreScale = udf(scoreScale_ _)

    // we need to apply the normalization for the population score as well
    // otherwise the final avg (score) won't be near 1
    val df_raw_score = spark.read.format("parquet").load(raw_score_path).cache()
    val minMax = df_raw_score.agg(
      elementWiseMinUdf('Score).alias("min_array"),
      elementWiseMaxUdf('Score).alias("max_array"),
      count("*").alias("cnt"))
      .collect()
    val minScore = minMax(0).getAs[Seq[Float]]("min_array")
    val maxScore = minMax(0).getAs[Seq[Float]]("max_array")
    val rowCnt = minMax(0).getAs[Long]("cnt")

    val df_normalized = df_raw_score
      .withColumn("min_array", lit(minScore))
      .withColumn("max_array", lit(maxScore))
      .withColumn("Score", scoreScale('Score, 'max_array, 'min_array))
      .drop("max_array", "min_array")

    val avgs = df_normalized
      .agg(elementWiseSumUdf('Score).alias("sum"))
      .withColumn("avg_array", transform(col("sum"), x => x / lit(rowCnt)))
      .collect()
    val avgScore = avgs(0).getAs[Seq[Float]]("avg_array")

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
    val df_seed_action_size = spark.read.parquet(policy_table_path)
      .where('Source === lit(DataSource.Seed.id) && 'CrossDeviceVendorId === lit(CrossDeviceVendor.None.id))
      .select('SourceId.as("SeedId"), 'ActiveSize)

    //min_max_avg.select("avg_array").crossJoin(df_seed_list.select('SeedId))
    df_seed_list.select('SeedId)
      .withColumn("avg_array", lit(avgScore))
      .withColumn("seed_avg_score", arrays_zip('avg_array, 'SeedId))
      .withColumn("seed_avg_score", explode('seed_avg_score))
      .select(col("seed_avg_score.avg_array").alias("PopulationSeedScoreRaw"), col("seed_avg_score.SeedId").alias("SeedId"))
      .join(df_seed_action_size, Seq("SeedId"))
      .withColumn("PopulationSeedScore", lit(1.0))
      .select("SeedId", "PopulationSeedScore", "ActiveSize", "PopulationSeedScoreRaw") // keep the raw population score, in case need it for debug
      .write
      .format("parquet")
      .mode("overwrite")
      .save(population_score_path)

  }

  def main(args: Array[String]): Unit = {
    runETLPipeline()
  }
}
