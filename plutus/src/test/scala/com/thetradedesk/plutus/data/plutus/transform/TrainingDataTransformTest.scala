package com.thetradedesk.plutus.data.plutus.transform

import org.scalatest._
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers._
import java.time.LocalDate

import com.thetradedesk.plutus.data.transform.TrainingDataTransform

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
    val (tr, tsV) = TrainingDataTransform.temporalPathSplits(paths)
    val expectedTrain = Seq("s3://bucket/prob/raw/google/year=2020/month=12/day=30","s3://bucket/prob/raw/google/year=2020/month=12/day=31")
    val expectedValTest = "s3://bucket/prob/raw/google/year=2020/month=12/day=1"

    println(tr)
    println(tsV)
    (tr == expectedTrain ) && (tsV == expectedValTest)

  }

  "intModelFeaturesCols" should "create a list of columns with hashing for string features" in {
    val output = TrainingDataTransform.intModelFeaturesCols(TrainingDataTransform.modelFeatures)
    println(output.mkString("Array(", ", ", ")"))
  }

  "modelTargetCols" should "create a list of columns for target cols" in {
    val output = TrainingDataTransform.modelTargetCols(TrainingDataTransform.modelTargets)
    println(output.mkString("Array(", ", ", ")"))
  }


  "outputPaths" should "create path with lookback and format" in {
    val result = TrainingDataTransform.outputDataPaths(
      s3Path = "s3://bucket",
      s3Prefix = "output",
      ttdEnv = "prod",
      svName = None,
      endDate = LocalDate.of(2021, 1, 1),
      lookBack = 2,
      dataFormat = "parquet",
      splitName = "train"
    )

    val expected = "s3://bucket/prob/raw/google/year=2021/month=1/day=1/lookback=2/format=parquet/train"
    result == expected


  }

  "outputPermutations" should "generate tuples for output" in {


//    val result = TrainingDataTransform.outputPermutations(null, null, null, Array(), Seq("parquet", "tfrecord"))

    //todo: tricky to test without creating a dataframe.
  }


}
