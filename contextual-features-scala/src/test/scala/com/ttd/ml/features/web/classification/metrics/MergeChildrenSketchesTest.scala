package com.ttd.ml.features.web.classification.metrics

import com.swoop.alchemy.spark.expressions.hll.functions.{hll_init_agg, hll_cardinality, hll_merge}
import com.ttd.ml.util.TTDSparkTest
import org.apache.spark.sql.functions.lit

import scala.math.abs

class MergeChildrenSketchesTest extends TTDSparkTest {
  import spark.implicits._
  private val EPSILON = 1e-5

  test("Merge depth 1 test") {
    val n = 10000
    val hllSketches = spark.range(n)
      // pre-aggregate
      .groupBy(('id % 3).as("id_mod10"))
      .agg(hll_init_agg('id, relativeSD = 0.02).as("hll_id"))
      .withColumn("bucket", lit(0))

    val pMap = Map(
      0 -> -1, // root node no parent
      1 -> 0, // two children both have parent 0
      2 -> 0
    )
    val childrenMerger = new MergeChildrenSketches(parentMap = pMap, buckets = Seq(0))
      .setIDCol("id_mod10")
      .setBucketCol("bucket")
      .setSketchCol("hll_id")

    val coherence = childrenMerger.transform(hllSketches)
    val actualDistribution = coherence
      .select('id_mod10, hll_cardinality('hll_id) as "size")
      .as[(Long, Long)]
      .collect()
      .sortBy(_._1)
      .map(_._2)

    val expectedDistribution = Array(n, n, n/3, n/3)
    val tolerance = 600
    assert(actualDistribution.length == expectedDistribution.length &&
      actualDistribution.zip(expectedDistribution).forall{case (a,b) => abs(a-b) < tolerance})
  }

  test("Merge depth 2 test") {
    val n = 10000
    val hllSketches = spark.range(n)
      // pre-aggregate
      .groupBy(('id % 5).as("id_mod10"))
      .agg(hll_init_agg('id, relativeSD = 0.02).as("hll_id"))
      .withColumn("bucket", lit(0))

    val pMap = Map(
      0 -> -1, // root node no parent
      1 -> 0, // root has two children both have parent 0
      2 -> 0,
      3 -> 1, // node 1 has two children also
      4 -> 1
    )
    val childrenMerger = new MergeChildrenSketches(parentMap = pMap, buckets = Seq(0))
      .setIDCol("id_mod10")
      .setBucketCol("bucket")
      .setSketchCol("hll_id")

    val coherence = childrenMerger.transform(hllSketches)
    val actualDistribution = coherence
      .select('id_mod10, hll_cardinality('hll_id) as "size")
      .as[(Long, Long)]
      .collect()
      .sortBy(_._1)
      .map(_._2)

    val expectedDistribution = Array(n, n, 3*(n/5), n/5, n/5, n/5)
    val tolerance = 600
    assert(actualDistribution.length == expectedDistribution.length &&
      actualDistribution.zip(expectedDistribution).forall{case (a,b) => abs(a-b) < tolerance})
  }

  test("Merge depth 2 test with missing buckets") {
    val n = 10000
    val hllSketches = spark.range(n)
      // pre-aggregate
      .groupBy(('id % 5).as("id_mod10"))
      .agg(hll_init_agg('id, relativeSD = 0.02).as("hll_id"))
      .withColumn("bucket", 'id_mod10 % 2)

    val pMap = Map(
      0 -> -1, // root node no parent
      1 -> 0, // root has two children both have parent 0
      2 -> 0,
      3 -> 1, // node 1 has two children also
      4 -> 1
    )
    val childrenMerger = new MergeChildrenSketches(parentMap = pMap, buckets = Seq(0, 1))
      .setIDCol("id_mod10")
      .setBucketCol("bucket")
      .setSketchCol("hll_id")

    val coherence = childrenMerger.transform(hllSketches)
    val actualDistribution = coherence
      .groupBy('id_mod10)
      .agg(hll_merge('hll_id).as("hll_id"))
      .select('id_mod10, hll_cardinality('hll_id) as "size")
      .as[(Long, Long)]
      .collect()
      .sortBy(_._1)
      .map(_._2)

    val expectedDistribution = Array(n, n, 3*(n/5), n/5, n/5, n/5)
    val tolerance = 600
    assert(actualDistribution.length == expectedDistribution.length &&
      actualDistribution.zip(expectedDistribution).forall{case (a,b) => abs(a-b) < tolerance})
  }
}
