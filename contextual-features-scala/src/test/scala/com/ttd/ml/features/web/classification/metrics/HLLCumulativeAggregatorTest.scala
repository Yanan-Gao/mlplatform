package com.ttd.ml.features.web.classification.metrics

import com.swoop.alchemy.spark.expressions.hll.functions.hll_init_agg
import com.ttd.ml.util.TTDSparkTest
import org.apache.spark.sql.functions.lit

import scala.math.abs

class HLLCumulativeAggregatorTest extends TTDSparkTest {
  import spark.implicits._
  private val EPSILON = 1e-5

  test("Cumulative Aggregation within tolerance") {
    val hllSketches = spark.range(10000)
      // pre-aggregate
      .groupBy(('id % 10).as("id_mod10"))
      .agg(hll_init_agg('id, relativeSD = 0.02).as("hll_id"))
      .withColumn("key", lit(0))

    val hllTransformer = new HLLCumulativeAggregator(numBuckets = 10)
      .setAggCol("key")
      .setBucketCol("id_mod10")
      .setSketchCol("hll_id")

    val coherence = hllTransformer.transform(hllSketches)
    val actualDistribution = coherence
      .select("cumulativeDistribution")
      .as[Seq[Long]]
      .head
    val expectedDistribution = (1 to 10) map (_*1000)

    val tolerance = 600
    assert(actualDistribution.size == expectedDistribution.size &&
            actualDistribution.zip(expectedDistribution).forall{case (a,b) => abs(a-b) < tolerance})
  }
}
