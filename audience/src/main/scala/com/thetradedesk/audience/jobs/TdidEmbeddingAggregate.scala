package com.thetradedesk.audience.jobs

import com.thetradedesk.audience.{date, dateFormatter, ttdEnv}
import com.thetradedesk.spark.TTDSparkContext.spark
import com.thetradedesk.spark.TTDSparkContext.spark.implicits._
import com.thetradedesk.spark.util.TTDConfig.config
import org.apache.spark.sql.functions._
import org.apache.spark.sql.catalyst.encoders.ExpressionEncoder
import org.apache.spark.sql.expressions.Aggregator
import org.apache.spark.sql.{Encoder, Encoders}

import java.nio.charset.StandardCharsets




object TdidEmbeddingAggregate {
  val salt = config.getString("salt", "TRM")
  val dateStr = date.format(dateFormatter)
  val br_emb_path= config.getString(
  "br_emb_path", s"s3://thetradedesk-mlplatform-us-east-1/data/${ttdEnv}/audience/RSMV2/emb/raw/v=1/date=${dateStr}/")
  val tdid_emb_path= config.getString(
    "tdid_emb_path", s"s3://thetradedesk-mlplatform-us-east-1/data/${ttdEnv}/audience/RSMV2/emb/agg/v=1/date=${dateStr}/")
  val decode_tdid = config.getBoolean("decode_tdid", true) // convert binary tdid format into string

  case class Buffer(sums: Array[Double], var count: Long)

  object RelevanceScoresAggregator
    extends Aggregator[Seq[Double], Buffer, Array[Double]] {

    val arrayLength=64
    // Initialize the buffer
    def zero: Buffer = Buffer(Array.fill(arrayLength)(0.0), 0L)

    // Add a new value to the buffer
    def reduce(buffer: Buffer, input: Seq[Double]): Buffer = {
      require(input.length == arrayLength,
        s"Input array length ${input.length} does not match expected length $arrayLength")

      for (i <- 0 until arrayLength) {
        buffer.sums.update(i, buffer.sums(i) + input(i))
      }
      buffer.count = buffer.count + 1
      buffer
    }

    // Merge two buffers
    def merge(b1: Buffer, b2: Buffer): Buffer = {
      for (i <- 0 until arrayLength) {
        b1.sums.update(i, b1.sums(i) + b2.sums(i))
      }

      b1.count = b1.count + b2.count

      b1
    }

    // Calculate final average
    def finish(buffer: Buffer): Array[Double] = {
      if (buffer.count == 0) {
        Array.fill(arrayLength)(0.0f)
      } else {
        buffer.sums.map(sum => (sum / buffer.count).toDouble)
      }
    }

    // Encoders for Spark to serialize the data
    def bufferEncoder: Encoder[Buffer] = Encoders.product[Buffer]
    def outputEncoder: Encoder[Array[Double]] = ExpressionEncoder[Array[Double]]
  }

  //spark.udf.register("emb_avg", udaf(RelevanceScoresAggregator))
  val emb_avg = udaf(RelevanceScoresAggregator)
  val utf8ToStringUdf = udf((bytes: Array[Byte]) => new String(bytes, StandardCharsets.UTF_8))

  def runETLPipeline(): Unit = {
    val rawEmb = spark.read.format("parquet").load(br_emb_path)
    val aggedEmb = rawEmb.groupBy("TDID")
      .agg(
        emb_avg(col("sen_pred")).alias("sen_pred_avg"),
        emb_avg(col("non_sen_pred")).alias("non_sen_pred_avg"),
        count("*").alias("cnt")
      )
      .withColumn("TDID", utf8ToStringUdf(col("TDID"))) //when(lit(decode_tdid), utf8ToStringUdf(col("TDID"))).otherwise(col("TDID")))
      .withColumn("split", abs(xxhash64(concat(col("TDID"), lit(salt)))) % lit(10))
      .select("TDID", "cnt", "sen_pred_avg", "non_sen_pred_avg", "split")

    aggedEmb
      .write
      .partitionBy("split")
      .format("parquet")
      .mode("overwrite")
      .option("codec", "org.apache.hadoop.io.compress.GzipCodec")
      .save(tdid_emb_path)

  }

  def main(args: Array[String]): Unit = {
    runETLPipeline()
  }
}
