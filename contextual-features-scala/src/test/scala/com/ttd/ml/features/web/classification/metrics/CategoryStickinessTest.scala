package com.ttd.ml.features.web.classification.metrics

import com.ttd.ml.util.TTDSparkTest

import scala.math.abs

class CategoryStickinessTest extends TTDSparkTest {
  import spark.implicits._
  private val EPSILON = 1e-5

  test("100% Stickiness of depth 1 categories") {
    val d1 = Seq(
      ("url", Seq(1,2))
    ).toDF("Url", "c1")

    val d2 = Seq(
      ("url", Seq(1,2))
    ).toDF("Url", "c2")

    val pMap = Map(
      0 -> -1, // root node no parent
      1 -> 0, // two children both have parent 0
      2 -> 0
    )

    val cs = new CategoryStickiness(
      levels = Seq(1),
      urlCol = "Url",
      predictionCol1 = "c1",
      predictionCol2 = "c2",
      parentMap = pMap
    )

    val actualStickiness = cs
      .transform(d1, d2)
      .select("Category", "Stickiness1")
      .as[(Long, Double)]
      .collect()
      .sortBy(_._1)
      .map(_._2)

    val expectedDistribution = Array(1.0, 1.0, 1.0)
    val tolerance = 1e-4
    assert(actualStickiness.length == expectedDistribution.length &&
      actualStickiness.zip(expectedDistribution).forall{case (a,b) => abs(a-b) < tolerance})
  }

  test("Stickiness at different depths with 1 url") {
    val d1 = Seq(
      ("url", Seq(1,2))
    ).toDF("Url", "c1")

    val d2 = Seq(
      ("url", Seq(3,4))
    ).toDF("Url", "c2")

    val pMap = Map(
      0 -> -1, // root node no parent
      1 -> 0, // root has two children both have parent 0
      2 -> 0,
      3 -> 1, // node 1 has two children also
      4 -> 1
    )

    val cs = new CategoryStickiness(
      levels = Seq(1),
      urlCol = "Url",
      predictionCol1 = "c1",
      predictionCol2 = "c2",
      parentMap = pMap
    )

    val actualStickiness = cs
      .transform(d1, d2)
      .select("Category", "Stickiness1")
      .as[(Long, Double)]
      .collect()
      .sortBy(_._1)
      .map(_._2)

    val expectedDistribution = Array(1.0, 1.0, 0.0, 0.0, 0.0)
    val tolerance = 1e-4
    assert(actualStickiness.length == expectedDistribution.length &&
      actualStickiness.zip(expectedDistribution).forall{case (a,b) => abs(a-b) < tolerance})
  }

  test("Stickiness at different depths with 2 urls") {
    val d1 = Seq(
      ("url1", Seq(1,2)),
      ("url2", Seq(1,2))
    ).toDF("Url", "c1")

    val d2 = Seq(
      ("url1", Seq(1,2)),
      ("url2", Seq(3,4))
    ).toDF("Url", "c2")

    val pMap = Map(
      0 -> -1, // root node no parent
      1 -> 0, // root has two children both have parent 0
      2 -> 0,
      3 -> 1, // node 1 has two children also
      4 -> 1
    )

    val cs = new CategoryStickiness(
      levels = Seq(1),
      urlCol = "Url",
      predictionCol1 = "c1",
      predictionCol2 = "c2",
      parentMap = pMap
    )

    val actualStickiness = cs
      .transform(d1, d2)
      .select("Category", "Stickiness1")
      .as[(Long, Double)]
      .collect()
      .sortBy(_._1)
      .map(_._2)

    val expectedDistribution = Array(1.0, 1.0, 0.5, 0.0, 0.0)
    val tolerance = 1e-4
    assert(actualStickiness.length == expectedDistribution.length &&
      actualStickiness.zip(expectedDistribution).forall{case (a,b) => abs(a-b) < tolerance})
  }
}
