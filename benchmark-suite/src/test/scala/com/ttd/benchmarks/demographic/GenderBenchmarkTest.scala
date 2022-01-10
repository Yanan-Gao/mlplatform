package com.ttd.benchmarks.demographic

import com.github.nscala_time.time.Imports._
import com.ttd.benchmarks.automl.NoSplit
import com.ttd.benchmarks.util.logging.DateRange
import com.ttd.benchmarks.util.{S3PathsUtil, TTDSparkTest}
import org.apache.spark.ml.classification.{LogisticRegression, LogisticRegressionModel}
import org.apache.spark.ml.linalg._
import org.apache.spark.ml.param.ParamMap
import org.apache.spark.ml.tuning.ParamGridBuilder

class GenderBenchmarkTest extends TTDSparkTest {
  import spark.implicits._

  test("Gender classification Benchmark test") {
    val lr = new LogisticRegression()
      .setMaxIter(10)
    val paramGrid: Array[ParamMap] = new ParamGridBuilder()
      .addGrid(lr.elasticNetParam, Array(0.0, 0.5, 1.0))
      .build()
    val now = DateTime.now()

    val dates = DateRange.range(now, now + 2.hour, 1.hour)

    val ageLabels = S3PathsUtil.mockReadRegionDate(
      spark.createDataFrame(Array(
        (1, 0),
        (2, 0),
        (3, 1)
      )).toDF("userId", "genderLabel")
    )

    val benchmarkTest = new GenderBenchmark[LogisticRegressionModel](lr, dates, paramGrid)
      .setParallelism(1)
      .setDateRegionDS(ageLabels)
      .setFeatureCol("features")
      .setTrainOrSplit(Left(NoSplit))

    val elems: Array[(Int, Vector)] = Array(
      (1, Vectors.dense(0.1)),
      (2, Vectors.dense(0.25)),
      (3, Vectors.dense(0.65))
    )
    val features = spark.createDataFrame(elems).toDF("userId", "features")

    val model = benchmarkTest.fit(features)
    val preds = model
      .transform(features)
      .select("userId", "prediction")
      .as[(Int, Double)]
      .collect()
      .sortBy(_._1)
      .map(_._2)

    preds should contain theSameElementsInOrderAs(Array(0.0, 0.0, 1.0))
  }
}
