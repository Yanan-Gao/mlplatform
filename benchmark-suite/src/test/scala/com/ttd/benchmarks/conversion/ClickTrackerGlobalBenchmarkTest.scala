package com.ttd.benchmarks.conversion

import com.github.nscala_time.time.Imports._

import com.ttd.benchmarks.util.{S3PathsUtil, TTDSparkTest}
import com.ttd.benchmarks.util.logging.DateRange
import org.apache.spark.ml.classification.{LogisticRegression, LogisticRegressionModel}
import org.apache.spark.ml.linalg.Vectors
import org.apache.spark.ml.param.ParamMap
import org.apache.spark.ml.tuning.ParamGridBuilder
import org.apache.spark.sql.functions.current_timestamp

class ClickTrackerGlobalBenchmarkTest extends TTDSparkTest {
  test("Click tracker Benchmark test") {
    val lr = new LogisticRegression()
      .setMaxIter(10)
    val paramGrid: Array[ParamMap] = new ParamGridBuilder()
      .addGrid(lr.regParam, Array(0.1, 0.01))
      .addGrid(lr.fitIntercept)
      .addGrid(lr.elasticNetParam, Array(0.0, 0.5, 1.0))
      .build()
    val now = DateTime.now()

    val dateSplits = TrainTestDateSplit(
      bidTrainDates   = DateRange.range(now, now + 2.hour, 1.hour),
      bidTestDates    = DateRange.range(now, now + 2.hour, 1.hour),
      clickTrainDates = DateRange.range(now, now + 3.hour, 1.hour),
      clickTestDates  = DateRange.range(now, now + 3.hour, 1.hour)
    )

    val bidDS = S3PathsUtil.mockReadDateHour(
      spark.createDataFrame(Array(
        (1, 1),
        (2, 1),
        (3, 2)
      )).toDF("BidRequestId", "CampaignId")
    )

    // Clicks are a subset of bids
    val clickDS = S3PathsUtil.mockReadDateHour(
      spark.createDataFrame(Array(
        (1, 1),
        (3, 2)
      )).toDF("BidRequestId", "CampaignId")
        .withColumn("LogEntryTime", current_timestamp())
    )

    val benchmarkTest = new ClickTrackerGlobalBenchmark[LogisticRegressionModel](lr, dateSplits, paramGrid)
      .setParallelism(1)
      .setClickTrackerDS(clickDS)
      .setBidDS(bidDS)
      .setFeatureCol("features")
    //      .setMinimumCampaignClickRate(0.6)

    val features = spark.createDataFrame(Array(
      (1, Vectors.dense(0.9)),
      (2, Vectors.dense(0.1)),
      (3, Vectors.dense(0.9))
    )).toDF("BidRequestId", "features")

    val model = benchmarkTest.fit(features)
    model
      .transform(features)
      .select("features", "prediction")
      .show()
  }
}
