package com.thetradedesk.data.transform

import org.scalatest._
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers._

import java.time.LocalDate

class TrainingDataTransformTest extends AnyFlatSpec {

  "inputDataPaths" should "create all the input paths" in {
    val result = TrainingDataTransform.inputDataPaths(
      s3Path = "s3://bucket",
      s3Prefix = "raw",
      ttdEnv = "prod",
      svName = None,
      endDate = LocalDate.of(2021, 1, 1),
      lookBack = None)

    val expected = Seq("s3://bucket/prob/raw/google/year=2021/month=1/day=1")
    result == expected
  }

  "inputDataPaths with lookback" should "create all the input paths" in {
    val result = TrainingDataTransform.inputDataPaths(
      s3Path = "s3://bucket",
      s3Prefix = "raw",
      ttdEnv = "prod",
      svName = None,
      endDate = LocalDate.of(2021, 1, 1),
      lookBack = Some(2))

    val expected = Seq("s3://bucket/prob/raw/google/year=2021/month=1/day=1",
      "s3://bucket/prob/raw/google/year=2020/month=12/day=31",
      "s3://bucket/prob/raw/google/year=2020/month=12/day=30")
    result == expected

    println(result.sorted.reverse.head)
    println(result.sorted)
  }

  "create temporal splits" should "" in {
    val paths = Seq("s3://bucket/prob/raw/google/year=2021/month=1/day=1",
      "s3://bucket/prob/raw/google/year=2020/month=12/day=31",
      "s3://bucket/prob/raw/google/year=2020/month=12/day=30")
    val (tr, v, ts) = TrainingDataTransform.createTemporalDataSplits(paths)
    val expectedTrain = Seq("s3://bucket/prob/raw/google/year=2020/month=12/day=30")
    val expectedVal = "s3://bucket/prob/raw/google/year=2020/month=12/day=31"
    val expectedTest = "s3://bucket/prob/raw/google/year=2021/month=1/day=1"

    println(tr)
    println(v)
    println(ts)
    (tr == expectedTrain ) && (v == expectedVal) && (ts == expectedTest)

  }
}
