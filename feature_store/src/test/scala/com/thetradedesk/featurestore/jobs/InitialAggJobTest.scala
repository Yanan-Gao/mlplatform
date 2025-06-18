package com.thetradedesk.featurestore.jobs

import com.thetradedesk.featurestore.configs.{AggDefinition, FieldAggSpec}
import com.thetradedesk.featurestore.features.Features.AggFunc
import com.thetradedesk.spark.TTDSparkContext.spark.implicits._
import org.apache.spark.sql.{Dataset, Row, SparkSession}
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

class InitialAggJobTest extends AnyFlatSpec with Matchers {
  private val spark: SparkSession = SparkSession.builder()
    .appName("InitialAggJobTest")
    .master("local[2]")
    .getOrCreate()

  // Test data setup
  private val testData = Seq(
    ("user1", "catA", "cat1", 10.0, Array(0, 3)),
    ("user1", "catB", "cat1", 10.0, Array(1, 0)),
    ("user1", "catA", "cat1", 10.0, Array(1, 2)),
    ("user1", "catA", "cat2", 0.0, Array(0, 0)),
    ("user2", "catA", "cat2", 30.0, Array(0, 0)),
    ("user2", "catB", "cat2", 40.0, Array(1, 2))
  ).toDF("user_id", "cat_1", "cat_2", "valueA", "values")

  private val testDataWithNulls = Seq(
    ("user1", "catA", "cat1", Some(10.0), Array(1, 2, 3, 0)),
    ("user1", "catB", "", Some(10.0), null),
    ("user1", null, "cat2", None, null),
    ("user2", "catA", "cat2", Some(30.0), null),
    ("user2", "catB", "cat2", null, null),
    ("user3", "catA", "cat2", null, null),
    ("user3", "catB", "cat2", null, null)
  ).toDF("user_id", "cat_1", "cat_2", "valueA", "values")

  // Helper methods
  private def createTestAggDefinition(spec: FieldAggSpec*): AggDefinition = {
    AggDefinition(
      datasource = "test",
      format = "parquet",
      rootPath = "/test/path",
      prefix = "test_prefix",
      outputRootPath = "/test/output",
      outputPrefix = "test_output",
      initOutputPrefix = "init_output",
      grain = "hour",
      aggregations = spec
    )
  }

  private def runAggregation(df: Dataset[_], aggDef: AggDefinition, saltSize: Int = 2) = {
    InitialAggJob.aggByDefinition(df, "user_id", aggDef, saltSize).collect().toList
  }

  // Test cases
  "InitialAggJob" should "correctly aggregate numeric values with Desc aggregation" in {
    val aggDef = createTestAggDefinition(FieldAggSpec(
      field = "valueA",
      dataType = "double",
      aggWindows = Seq(1),
      windowUnit = "hour",
      aggFuncs = Seq(AggFunc.Desc)
    ))

    val result = runAggregation(testData, aggDef)
    
    result should contain theSameElementsAs Seq(
      Row("user1", Map("count" -> 4, "nonZeroCount" -> 3, "sum" -> 30, "min" -> 0, "max" -> 10, "allNull" -> 0)),
      Row("user2", Map("count" -> 2, "nonZeroCount" -> 2, "sum" -> 70, "min" -> 30, "max" -> 40, "allNull" -> 0))
    )
  }

  it should "correctly aggregate categorical values with Frequency aggregation" in {
    val aggDef = createTestAggDefinition(FieldAggSpec(
      field = "cat_1",
      dataType = "string",
      aggWindows = Seq(1),
      windowUnit = "hour",
      aggFuncs = Seq(AggFunc.Frequency)
    ))

    val result = runAggregation(testData, aggDef, 4)
    
    result should contain theSameElementsAs Seq(
      Row("user1", Map("catB" -> 1, "catA" -> 3.0)),
      Row("user2", Map("catA" -> 1.0, "catB" -> 1))
    )
  }

  it should "correctly aggregate categorical values with TopN aggregation" in {
    val aggDef = createTestAggDefinition(FieldAggSpec(
      field = "cat_2",
      dataType = "string",
      aggWindows = Seq(1),
      windowUnit = "hour",
      topN = 1,
      aggFuncs = Seq(AggFunc.TopN)
    ))

    val result = runAggregation(testData, aggDef, 1)
    
    result should contain theSameElementsAs Seq(
      Row("user1", Map("cat1" -> 3)),
      Row("user2", Map("cat2" -> 2))
    )
  }

  it should "correctly aggregate array values with Desc aggregation" in {
    val aggDef = createTestAggDefinition(FieldAggSpec(
      field = "values",
      dataType = "array_int",
      aggWindows = Seq(1),
      windowUnit = "hour",
      aggFuncs = Seq(AggFunc.Desc)
    ))

    val result = runAggregation(testData, aggDef)
    
    result should contain theSameElementsAs Seq(
      Row("user1", Map("count" -> 8, "nonZeroCount" -> 4, "sum" -> 7, "min" -> 0, "max" -> 3, "allNull" -> 0)),
      Row("user2", Map("count" -> 4, "nonZeroCount" -> 2, "sum" -> 3, "min" -> 0, "max" -> 2, "allNull" -> 0))
    )
  }

  it should "correctly handle null values in aggregation" in {
    val aggDef = createTestAggDefinition(
      FieldAggSpec(
        field = "valueA",
        dataType = "double",
        aggWindows = Seq(1),
        windowUnit = "hour",
        aggFuncs = Seq(AggFunc.Desc)
      ),
      FieldAggSpec(
        field = "values",
        dataType = "array_int",
        aggWindows = Seq(1),
        windowUnit = "hour",
        aggFuncs = Seq(AggFunc.Desc)
      )
    )

    val result = runAggregation(testDataWithNulls, aggDef, 2)
    
    result should contain theSameElementsAs Seq(
      Row("user1", 
        Map("sum" -> 20, "count" -> 2, "nonZeroCount" -> 2, "min" -> 10, "max" -> 10, "allNull" -> 0),
        Map("count" -> 4, "nonZeroCount" -> 3, "sum" -> 6, "min" -> 0, "max" -> 3, "allNull" -> 0)
      ),
      Row("user2", 
        Map("sum" -> 30, "count" -> 1, "nonZeroCount" -> 1, "min" -> 30, "max" -> 30, "allNull" -> 0),
        Map("count" -> 0, "nonZeroCount" -> 0, "sum" -> 0, "min" -> 0, "max" -> 0, "allNull" -> 1)
      ),
      Row("user3", 
        Map("sum" -> 0, "count" -> 0, "nonZeroCount" -> 0, "min" -> 0, "max" -> 0, "allNull" -> 1),
        Map("count" -> 0, "nonZeroCount" -> 0, "sum" -> 0, "min" -> 0, "max" -> 0, "allNull" -> 1)
      )
    )
  }
}
