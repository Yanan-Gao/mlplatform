package com.ttd.ml.features.web.classification.metrics

import com.swoop.alchemy.spark.expressions.hll.functions.{hll_cardinality, hll_merge, hll_row_merge}
import com.ttd.ml.spark.TTDSparkContext.spark.implicits._
import org.apache.spark.ml.Transformer
import org.apache.spark.ml.param.{Param, ParamMap}
import org.apache.spark.ml.util.Identifiable
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.apache.spark.sql.{DataFrame, Dataset}

/* Merges children sketches given a map of child id to parent id */
class MergeChildrenSketches(parentMap: Map[Int, Int], buckets: Seq[Int]) extends Transformer {
  override val uid: String = Identifiable.randomUID("MergeChildrenSketches")

  final val sketchCol = new Param[String](this, "hllSketch", "The hyperloglog sketch column")
  final val idCol = new Param[String](this, "idCol", "The id column")
  final val bucketCol = new Param[String](this, "bucketCol", "The bucket column to cumulatively aggregate over")

  final case class PairCount(pair: (String, String), count: Int)

  def setSketchCol(value: String): this.type = set(sketchCol, value)
  setDefault(sketchCol -> "hll_id")
  def setIDCol(value: String): this.type = set(idCol, value)
  setDefault(idCol -> "UniqueID")
  def setBucketCol(value: String): this.type = set(bucketCol, value)
  setDefault(bucketCol -> "bucket")

  def depth: Int => Int = id => {
    if (!parentMap.contains(id)) {
      0
    } else {
      1 + depth(parentMap.apply(id))
    }
  }

  def getParent: Int => Int = id => {
    parentMap.getOrElse(id, -1)
  }

  override def transform(ds: Dataset[_]): DataFrame = {
    // Fill all buckets with null to handle missing values using cross join
    val bucketsDF = buckets.toDF($(bucketCol))
    val pMap = parentMap.toSeq.toDF($(idCol), "Parent")
    val resultTable = pMap.crossJoin(bucketsDF)
    var mergedSketches = ds
      .join(resultTable, Seq($(idCol), $(bucketCol)), "outer")
      .withColumn("depth", udf(depth).apply(col($(idCol))))
      .cache

    // Find max depth and aggregate children sketches for each depth level starting with largest depth
    val maxDepth = mergedSketches.agg(max('depth)).as[Int].collect().head
    for (currDepth <- maxDepth to 1 by -1) {
//      println(currDepth)
      // aggregate children sketches by parent and bucket
      val childrenMergedSketches = mergedSketches
        .filter('depth === currDepth)
        .groupBy("Parent", $(bucketCol))
        .agg(hll_merge(col($(sketchCol))) as "hll_id_children")

//      childrenMergedSketches
//          .select(col("Parent"), col($(bucketCol)), hll_cardinality('hll_id_children) as "size")
//          .show(false)

      // merge the children sketches with the parent
      mergedSketches = mergedSketches
        .join(childrenMergedSketches
          .withColumnRenamed("Parent", $(idCol)),
          Seq($(idCol), $(bucketCol)), "outer")
        .select(col($(idCol)), 'Parent, col($(bucketCol)), 'depth,
          hll_row_merge(col($(sketchCol)), 'hll_id_children) as $(sketchCol))

//      mergedSketches
//        .select(col($(idCol)), col($(bucketCol)), hll_cardinality($(sketchCol)) as "size")
//        .show(false)
    }
    mergedSketches
      .drop("Parent")
  }

  override def transformSchema(schema: StructType): StructType = {
    schema
  }

  override def copy(extra: ParamMap): MergeChildrenSketches = {
    defaultCopy(extra)
  }
}


