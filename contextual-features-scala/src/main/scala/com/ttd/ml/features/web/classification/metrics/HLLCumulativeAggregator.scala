package com.ttd.ml.features.web.classification.metrics

import com.swoop.alchemy.spark.expressions.hll.functions.{hll_cardinality, hll_row_merge}
import org.apache.spark.ml.Transformer
import org.apache.spark.ml.param.{Param, ParamMap}
import org.apache.spark.ml.util.Identifiable
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.apache.spark.sql.{DataFrame, Dataset}

/* Aggregates HyperLogLog Sketches over buckets cumulatively */
class HLLCumulativeAggregator(numBuckets: Int) extends Transformer {
  override val uid: String = Identifiable.randomUID("HLLCumulativeAggregator")

  final val sketchCol = new Param[String](this, "hllSketch", "The hyperloglog sketch column")
  final val aggCol = new Param[String](this, "aggCol", "The aggregate key column")
  final val bucketCol = new Param[String](this, "bucketCol", "The bucket column to cumulatively aggregate over")

  final case class PairCount(pair: (String, String), count: Int)

  def setSketchCol(value: String): this.type = set(sketchCol, value)
  setDefault(sketchCol -> "hllSketch")
  def setAggCol(value: String): this.type = set(aggCol, value)
  setDefault(aggCol -> "key")
  def setBucketCol(value: String): this.type = set(bucketCol, value)
  setDefault(bucketCol -> "bucket")

  private def cumulativeMerge(df: DataFrame, columnToMerge: String): DataFrame = {
    (1 to numBuckets).foldLeft(df.withColumn(s"${columnToMerge}_0", col(columnToMerge)(0)))((dff, k) =>
      dff
        .withColumn(s"${columnToMerge}_$k",
          hll_row_merge(col(s"${columnToMerge}_${k-1}"), col(columnToMerge)(k))
        )
        .withColumn(s"${columnToMerge}_${k-1}", hll_cardinality(s"${columnToMerge}_${k-1}"))
    ).withColumn(s"${columnToMerge}_$numBuckets", hll_cardinality(s"${columnToMerge}_$numBuckets"))
  }

  override def transform(ds: Dataset[_]): DataFrame = {
    cumulativeMerge(
      ds
      .groupBy($(aggCol))
      .agg(array_sort(collect_list(struct($(bucketCol), $(sketchCol)))).getField($(sketchCol)) as "ids"),
      "ids")
      .withColumn("cumulativeDistribution", array((0 until numBuckets).map(i => col(s"ids_$i")) :_*))
      .drop(Seq("ids") ++ (0 to numBuckets).map(i => s"ids_$i") :_* ) // drop temporary columns
  }

  override def transformSchema(schema: StructType): StructType = {
    schema
        .add(StructField("cumulativeDistribution", ArrayType(DoubleType), nullable = false))
  }

  override def copy(extra: ParamMap): HLLCumulativeAggregator = {
    defaultCopy(extra)
  }
}


