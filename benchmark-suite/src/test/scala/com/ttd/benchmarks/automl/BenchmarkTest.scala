package com.ttd.benchmarks.automl

import com.github.nscala_time.time
import com.github.nscala_time.time.Imports
import com.github.nscala_time.time.Imports._
import com.ttd.benchmarks.automl.Benchmark
import com.ttd.benchmarks.conversion.{ClickTrackerGlobalBenchmark, ClickTrackerSwarmBenchmark, TrainTestDateSplit}
import com.ttd.benchmarks.util.{S3PathsUtil, TTDSparkTest}
import com.ttd.benchmarks.util.logging.{DateHourPartitionedDataset, DateRange}
import ml.dmlc.xgboost4j.scala.spark.{XGBoostClassificationModel, XGBoostClassifier, XGBoostRegressionModel, XGBoostRegressor}
import org.apache.spark.ml.classification.{LogisticRegression, LogisticRegressionModel}
import org.apache.spark.ml.evaluation.{BinaryClassificationEvaluator, MulticlassClassificationEvaluator, RegressionEvaluator}
import org.apache.spark.ml.linalg.Vectors
import org.apache.spark.ml.param.ParamMap
import org.apache.spark.ml.regression.{LinearRegression, LinearRegressionModel}
import org.apache.spark.ml.tuning.ParamGridBuilder
import org.apache.spark.sql.{DataFrame, SparkSession, functions}
import org.apache.spark.sql.functions.current_timestamp

class BenchmarkTest extends TTDSparkTest {
  import spark.implicits._

  test("Can train simple regression model") {
    val data = (1 to 100 map (i => (i, Vectors.dense(i, 2.0, 3.0))))
      .toDF("label", "features")
    val Array(training, test) = data.randomSplit(Array(0.8, 0.2), seed = 12345)

    val lr = new LinearRegression()
      .setMaxIter(10)

    val evaluator = new RegressionEvaluator().setMetricName("rmse")

    val paramGrid: Array[ParamMap] = new ParamGridBuilder()
      .addGrid(lr.regParam, Array(0.1, 0.01))
      .addGrid(lr.fitIntercept)
      .addGrid(lr.elasticNetParam, Array(0.0, 0.5, 1.0))
      .build()

    val rb = new Benchmark[LinearRegressionModel](lr, paramGrid, evaluator)
    val bestModel = rb.fit(training)

    bestModel
      .transform(test)
      .select("features", "label", "prediction")
      .show()

    succeed
  }

  test("Can train xgboost regression model") {
    val data = (1 to 100 map (i => (i, Vectors.dense(i, 2.0, 3.0))))
      .toDF("label", "features")
    val Array(training, test) = data.randomSplit(Array(0.8, 0.2), seed = 12345)

    val lr = new XGBoostRegressor()

    val evaluator = new RegressionEvaluator().setMetricName("rmse")

    val paramGrid: Array[ParamMap] = new ParamGridBuilder()
      .addGrid(lr.maxDepth, Array(6))
      .addGrid(lr.numRound, Array(1,2,4,8))
      .build()

    val rb = new Benchmark[XGBoostRegressionModel](lr, paramGrid, evaluator)
    val bestModel = rb.fit(training)

    bestModel
      .transform(test)
      .select("features", "label", "prediction")
      .show()

    succeed
  }

  test("Can train simple classification model") {
    val data = (0 until 100 map (i => ((i / 50).toInt, Vectors.dense(i, 2.0, 3.0))))
      .toDF("label", "features")
    val Array(training, test) = data.randomSplit(Array(0.8, 0.2), seed = 12345)

    val lr = new LogisticRegression()
      .setMaxIter(10)

    val evaluator = new BinaryClassificationEvaluator().setMetricName("areaUnderROC")

    val paramGrid: Array[ParamMap] = new ParamGridBuilder()
      .addGrid(lr.regParam, Array(0.1, 0.01))
      .addGrid(lr.fitIntercept)
      .addGrid(lr.elasticNetParam, Array(0.0, 0.5, 1.0))
      .build()

    val rb = new Benchmark[LogisticRegressionModel](lr, paramGrid, evaluator)
    val bestModel = rb.fit(training)

    bestModel
      .transform(test)
      .select("features", "label", "prediction")
      .show()

    succeed
  }

  test("Can train xgboost classification model") {
    val data = (0 until 100 map (i => ((i / 50).toInt, Vectors.dense(i, 2.0, 3.0))))
      .toDF("label", "features")
    val Array(training, test) = data.randomSplit(Array(0.8, 0.2), seed = 12345)

    val lr = new XGBoostClassifier()

    val evaluator = new MulticlassClassificationEvaluator()
      .setMetricName("accuracy");

    val paramGrid: Array[ParamMap] = new ParamGridBuilder()
      .addGrid(lr.maxDepth, Array(6))
      .addGrid(lr.numRound, Array(1,2,4))
      .build()

    val rb = new Benchmark[XGBoostClassificationModel](lr, paramGrid, evaluator)
    val bestModel = rb.fit(data)

    bestModel
      .transform(test)
      .select("features", "label", "prediction")
      .show()

    succeed
  }
}
