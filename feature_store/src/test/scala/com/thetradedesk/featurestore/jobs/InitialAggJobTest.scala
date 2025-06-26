package com.thetradedesk.featurestore.jobs

import com.thetradedesk.featurestore.configs.{AggDefinition, FieldAggSpec}
import com.thetradedesk.featurestore.features.Features.AggFunc
import com.thetradedesk.featurestore.jobs.AggDefinitionHelper.createTestAggDefinition
import com.thetradedesk.featurestore.testutils.TTDSparkTest
import com.thetradedesk.spark.TTDSparkContext.spark.implicits._
import org.apache.spark.sql.{Dataset, Row}
import org.scalatest.matchers.must.Matchers.contain
import org.scalatest.matchers.should.Matchers.convertToAnyShouldWrapper

object AggDefinitionHelper {
  // Helper methods
  def createTestAggDefinition(spec: FieldAggSpec*): AggDefinition = {
    AggDefinition(
      datasource = "test",
      format = "parquet",
      rootPath = "/test/path",
      prefix = "test_prefix",
      outputRootPath = "/test/output",
      outputPrefix = "test_output",
      initOutputPrefix = "init_output",
      grain = "hour",
      aggLevels = Set("user_id"),
      aggregations = spec
    )
  }

  def nullDesc: Map[String, Double] = Map("count" -> 0, "nonZeroCount" -> 0, "sum" -> 0, "min" -> 0, "max" -> 0, "allNull" -> 1)
}

class InitialAggJobTest extends TTDSparkTest {

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

  private def runAggregation(df: Dataset[_], aggDef: AggDefinition, saltSize: Int = 2) = {
    InitialAggJob.aggByDefinition(df, "user_id", aggDef, saltSize).collect().toList
  }

  // Test cases
  test("correctly aggregate numeric values with Desc aggregation") {
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

  test("correctly aggregate categorical values with Frequency aggregation") {
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

  test("correctly aggregate categorical values with TopN aggregation") {
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

  test("correctly aggregate array values with Desc aggregation") {
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

  test("correctly handle null values in aggregation") {
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


  test("correctly aggregate vector values with Desc aggregation") {
    val testDataVector = Seq(
      ("user1", Array(1, 2)),
      ("user1", Array(11, 12)),
      ("user2", Array(2, 3)),
      ("user2", Array(22, 23)),
      ("user3", Array(3, 4))
    ).toDF("user_id", "values")

    val aggDef = createTestAggDefinition(FieldAggSpec(
      field = "values",
      dataType = "array_int",
      arraySize = 2,
      aggWindows = Seq(1),
      windowUnit = "hour",
      aggFuncs = Seq(AggFunc.VectorDesc)
    ))

    val result = runAggregation(testDataVector, aggDef)

    result should contain theSameElementsAs Seq(
      Row("user1", Seq(
        Seq(2, 2), // counts
        Seq(12, 14), // sums
        Seq(1, 2), // mins
        Seq(11, 12)) // maxs
      ),
      Row("user2", Seq(
        Seq(2, 2),
        Seq(24, 26),
        Seq(2, 3),
        Seq(22, 23))
      ),
      Row("user3", Seq(
        Seq(1, 1),
        Seq(3, 4),
        Seq(3, 4),
        Seq(3, 4),
      ))
    )
  }

  test("correctly aggregate vector values with null values") {
    val testDataVector = Seq(
      ("user1", None),
      ("user1", Some(Array(11, 12))),
      ("user2", None),
      ("user2", None),
      ("user3", None)
    ).toDF("user_id", "values")

    val aggDef = createTestAggDefinition(FieldAggSpec(
      field = "values",
      dataType = "array_int",
      arraySize = 2,
      aggWindows = Seq(1),
      windowUnit = "hour",
      aggFuncs = Seq(AggFunc.VectorDesc)
    ))

    val result = runAggregation(testDataVector, aggDef)

    result should contain theSameElementsAs Seq(
      Row("user1", Seq(Seq(1, 1), Seq(11, 12), Seq(11, 12), Seq(11, 12))),
      Row("user2", null),
      Row("user3", null)
    )
  }
}
