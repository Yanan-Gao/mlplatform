package com.ttd.benchmarks.demographic

import com.github.nscala_time.time.Imports._
import com.ttd.benchmarks.automl.NoSplit
import com.ttd.benchmarks.util.logging.DateRange
import com.ttd.benchmarks.util.{S3PathsUtil, TTDSparkTest}
import org.apache.spark.ml.linalg.Vectors
import org.apache.spark.ml.param.ParamMap
import org.apache.spark.ml.regression.{LinearRegression, LinearRegressionModel}
import org.apache.spark.ml.tuning.ParamGridBuilder

class AgeBenchmarkTest extends TTDSparkTest {
  import spark.implicits._

  test("Age regression Benchmark test") {
    val lr = new LinearRegression()
      .setMaxIter(10)
    val paramGrid: Array[ParamMap] = new ParamGridBuilder()
      .addGrid(lr.elasticNetParam, Array(0.0, 0.5, 1.0))
      .build()
    val now = DateTime.now()

    val dates = DateRange.range(now, now + 2.hour, 1.hour)

    val ageLabels = S3PathsUtil.mockReadRegionDate(
      spark.createDataFrame(Array(
        (1, 10),
        (2, 30),
        (3, 60)
      )).toDF("userId", "ageLabel")
    )

    val benchmarkTest = new AgeBenchmark[LinearRegressionModel](lr, dates, paramGrid)
      .setParallelism(1)
      .setDateRegionDs(ageLabels)
      .setFeatureCol("features")
      .setTrainOrSplit(Left(NoSplit))

    val features = spark.createDataFrame(Array(
      (1, Vectors.dense(0.1)),
      (2, Vectors.dense(0.25)),
      (3, Vectors.dense(0.65))
    )).toDF("userId", "features")

    val model = benchmarkTest.fit(features)

    val preds = model
      .transform(features)
      .select("userId", "prediction")
      .as[(Int, Double)]
      .collect()
      .sortBy(_._1)
      .map(_._2)

    val tol = 10 // allow tolerance of 10 years
    assert(preds.zip(Array(10, 30, 60)).forall{case (pred, actual) => Math.abs(pred-actual) < tol})
  }
}
