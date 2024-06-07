package com.thetradedesk.audience.jobs

import com.thetradedesk.audience.datasets._
import com.thetradedesk.audience.dateTime
import com.thetradedesk.spark.TTDSparkContext.spark
import com.thetradedesk.spark.util.prometheus.PrometheusClient
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import java.time.LocalDate

object DiagnosisDataMonitor {
  val prometheus = new PrometheusClient("DiagnosisJob", "DiagnosisDataMonitor")
  val diagnosisMetricsCount = prometheus.createGauge("distributed_algo_diagnosis_metrics_count", "Hourly counts of metrics from distributed algo diagnosis pipeline", "roigoaltype", "type")
  val CountMetricsSchema = new StructType()
    .add("RSMErrorCount", IntegerType, true)
    .add("RMaxBidCount", IntegerType, true)
    .add("RelevanceSourceNoneCount", IntegerType, true)
    .add("RelevanceSourceDefaultCount", IntegerType, true)
    .add("RelevanceSourceNoSeedCount", IntegerType, true)
    .add("RelevanceSourceNoSyntheticIdCount", IntegerType, true)
    .add("RelevanceSourceNoEmbeddingCount", IntegerType, true)

  def unpivot(df: DataFrame, key_cols: Seq[String], pivot_cols: Seq[String]): DataFrame = {
    df.withColumn("key", array(pivot_cols.map(lit(_)): _*))
      .withColumn("value", array(pivot_cols.map(col(_)): _*))
      .withColumn("combine", arrays_zip(col("key"), col("value")))
      .withColumn("combine", explode(col("combine")))
      .select((key_cols ++ Seq("combine.key", "combine.value")).map(col(_)): _*)
  }

  def generateMetrics(date: LocalDate, hour: Int): DataFrame = {
    val roiGoalTypeData = ROIGoalTypeDataset()
      .readLatestPartition()

    val diagnosisData = DiagnosisReadableDataset()
      .readPartitionHourly(date, Seq(hour))(spark)
      .filter(col("TotalCount") > 10)
      .withColumn("metrics", from_json(col("CountMetrics"), CountMetricsSchema))
      .groupBy("ROIGoalTypeId")
      .agg(
        count("*").as("total"),
        count(
          when(col("metrics.RSMErrorCount") > col("TotalCount") * 0.1 &&
            !(col("metrics.RelevanceSourceNoSeedCount") + col("metrics.RelevanceSourceNoneCount") === col("TotalCount")) &&
            !(col("metrics.RelevanceSourceNoSyntheticIdCount") + col("metrics.RelevanceSourceNoneCount") === col("TotalCount")) &&
            !(col("metrics.RelevanceSourceNoEmbeddingCount") + col("metrics.RelevanceSourceNoneCount") === col("TotalCount"))
            , 1).otherwise(null)
        ).as("rsmerror"),
        count(when(col("metrics.RMaxBidCount") > col("TotalCount") * 0.2, 1).otherwise(null)).as("rmaxbid"),
        count(when(col("metrics.RelevanceSourceDefaultCount") > col("TotalCount") * 0.2, 1).otherwise(null)).as("rsmdefault")
      )
      .join(roiGoalTypeData, Seq("ROIGoalTypeId"), "left")

    val pivot_cols = Seq("total", "rsmerror", "rmaxbid", "rsmdefault")
    unpivot(diagnosisData, Seq("ROIGoalTypeName"), pivot_cols)
  }

  def writeMetricsToProm(df: DataFrame): Unit = {
    df.take(df.count.toInt).foreach(row =>
      diagnosisMetricsCount.labels(row.getAs[String]("ROIGoalTypeName"), row.getAs[String]("key"))
        .set(row.getAs[Long]("value"))
    )
  }

  def runETLPipeline(): Unit = {
    val date = dateTime.toLocalDate
    val hour = dateTime.getHour

    val df = generateMetrics(date, hour)
    writeMetricsToProm(df)
  }

  def main(args: Array[String]): Unit = {
    runETLPipeline()
    prometheus.pushMetrics()
  }
}