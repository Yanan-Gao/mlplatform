package com.thetradedesk.featurestore.jobs

import com.tdunning.math.stats.MergingDigest
import com.thetradedesk.featurestore.aggfunctions.AggFunctions.AggFuncV2
import com.thetradedesk.featurestore.configs._
import com.thetradedesk.featurestore.jobs.AggDefinitionHelper.createTestAggDefinition
import com.thetradedesk.featurestore.rsm.CommonEnums.Grain
import com.thetradedesk.featurestore.testutils.TTDSparkTest
import com.thetradedesk.spark.TTDSparkContext.spark.implicits._
import org.apache.spark.sql.{Dataset, Row}
import org.scalactic.Tolerance.convertNumericToPlusOrMinusWrapper
import org.scalatest.matchers.must.Matchers.{be, contain}
import org.scalatest.matchers.should.Matchers.convertToAnyShouldWrapper

import java.nio.ByteBuffer

object AggDefinitionHelper {

  val defaultAggLevel = AggLevelConfig("user_id", 4, initAggGrains = Array(Grain.Hourly), initWritePartitions = Some(1), enableFeatureKeyCount = false)

  // Helper methods
  def createTestAggDefinition(spec: FieldAggSpec*): AggDefinition = {
    AggDefinition(
      dataSource = DataSourceConfig(
        name = "test",
        rootPath = "/test/path",
        prefix = "test_prefix",
      ),
      aggLevels = Set(defaultAggLevel),
      initAggConfig = AggTaskConfig(outputRootPath = "/test/InitAgg", outputPrefix = "test_output"),
      rollupAggConfig = AggTaskConfig(outputRootPath = "/test/RollupAgg", outputPrefix = "test_output", windowGrain = Some(Grain.Daily)),
      aggregations = spec
    )
  }
}

class InitialAggJobTest extends TTDSparkTest {

  override def beforeAll(): Unit = {
    super.beforeAll()
    // Set system properties before the object is initialized
    System.setProperty("grain", "hour")
  }

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
    InitialAggJob.aggByDefinition(df, aggDef, AggLevelConfig("user_id", saltSize, Array(Grain.Hourly), enableFeatureKeyCount = false)).collect().toList
  }

  // Test cases
  test("correctly aggregate numeric values with Desc aggregation and random salt") {
    val aggDef = createTestAggDefinition(FieldAggSpec(
      field = "valueA",
      dataType = "double",
      aggWindows = Seq(1),
      aggFuncs = Seq(AggFuncV2.Count, AggFuncV2.Sum, AggFuncV2.Min, AggFuncV2.Max, AggFuncV2.NonZeroCount)
    ))

    val result = runAggregation(testData, aggDef) // agg with random salt first then merge result

    result should contain theSameElementsAs Seq(
      Row("user1", 4, 30, 0, 10, 3),
      Row("user2", 2, 70, 30, 40, 2)
    )
  }

  test("correctly aggregate numeric values with Desc aggregation but no random salt") {
    val aggDef = createTestAggDefinition(FieldAggSpec(
      field = "valueA",
      dataType = "double",
      aggWindows = Seq(1),
      aggFuncs = Seq(AggFuncV2.Count, AggFuncV2.Sum, AggFuncV2.Min, AggFuncV2.Max, AggFuncV2.NonZeroCount)
    ))

    val result = runAggregation(testData, aggDef, -1) // direct agg

    result should contain theSameElementsAs Seq(
      Row("user1", 4, 30, 0, 10, 3),
      Row("user2", 2, 70, 30, 40, 2)
    )
  }

  test("correctly aggregate categorical values with Frequency aggregation") {
    val aggDef = createTestAggDefinition(FieldAggSpec(
      field = "cat_1",
      dataType = "string",
      aggWindows = Seq(1),
      aggFuncs = Seq(AggFuncV2.Frequency())
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
      aggFuncs = Seq(AggFuncV2.Frequency(Array(1)))
    ))

    val result = runAggregation(testData, aggDef.extractInitialAggDefinition(), 1)

    result should contain theSameElementsAs Seq(
      Row("user1", Map("cat1" -> 3, "cat2" -> 1)), // since we applied the multiplier, so result should contains all cateA values
      Row("user2", Map("cat2" -> 2))
    )
  }

  test("correctly aggregate array values with Desc aggregation") {
    val aggDef = createTestAggDefinition(FieldAggSpec(
      field = "values",
      dataType = "array_int",
      aggWindows = Seq(1),
      aggFuncs = Seq(AggFuncV2.Count, AggFuncV2.NonZeroCount, AggFuncV2.Sum, AggFuncV2.Min, AggFuncV2.Max)
    ))

    val result = runAggregation(testData, aggDef)

    result should contain theSameElementsAs Seq(
      Row("user1", 8, 4, 7, 0, 3),
      Row("user2", 4, 2, 3, 0, 2)
    )
  }

  test("correctly handle null values in aggregation") {
    val aggDef = createTestAggDefinition(
      FieldAggSpec(
        field = "valueA",
        dataType = "double",
        aggWindows = Seq(1),
        aggFuncs = Seq(AggFuncV2.Count, AggFuncV2.NonZeroCount, AggFuncV2.Sum, AggFuncV2.Min, AggFuncV2.Max)
      ),
      FieldAggSpec(
        field = "values",
        dataType = "array_int",
        aggWindows = Seq(1),
        aggFuncs = Seq(AggFuncV2.Count, AggFuncV2.NonZeroCount, AggFuncV2.Sum, AggFuncV2.Min, AggFuncV2.Max)
      )
    )

    val result = runAggregation(testDataWithNulls, aggDef)

    result should contain theSameElementsAs Seq(
      Row("user1", 2, 2, 20.0, 10.0, 10.0, 4, 3, 6.0, 0.0, 3.0),
      Row("user2", 1, 1, 30, 30, 30, null, null, null, null, null),
      Row("user3", 0, 0, null, null, null, null, null, null, null, null)
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
      dataType = "vector",
      arraySize = 2,
      aggWindows = Seq(1),
      aggFuncs = Seq(AggFuncV2.Count, AggFuncV2.Sum, AggFuncV2.Min, AggFuncV2.Max)
    ))

    val result = runAggregation(testDataVector, aggDef)
    result should contain theSameElementsAs Seq(
      Row("user1", 2, Seq(12, 14), Seq(1, 2), Seq(11, 12)),
      Row("user2", 2, Seq(24, 26), Seq(2, 3), Seq(22, 23)),
      Row("user3", 1, Seq(3, 4), Seq(3, 4), Seq(3, 4))
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
      dataType = "vector",
      arraySize = 2,
      aggWindows = Seq(1),
      aggFuncs = Seq(AggFuncV2.Count, AggFuncV2.Sum, AggFuncV2.Min, AggFuncV2.Max)
    ))

    val result = runAggregation(testDataVector, aggDef)

    result should contain theSameElementsAs Seq(
      Row("user1", 1, Seq(11, 12), Seq(11, 12), Seq(11, 12)),
      Row("user2", 0, null, null, null),
      Row("user3", 0, null, null, null)
    )
  }

  test("correctly aggregate quantile summary") {
    val testData = (1 to 1000).map(i => ("user1", i.toDouble)).toDF("user_id", "value")

    val aggDef = createTestAggDefinition(FieldAggSpec(
      field = "value",
      dataType = "double",
      aggWindows = Seq(1),
      aggFuncs = Seq(AggFuncV2.QuantileSummary)
    ))

    val result = runAggregation(testData, aggDef)
    val quantileSummaryCol = result.head.asInstanceOf[Row].getAs[Array[Byte]]("value_QuantileSummary")
    val digest = MergingDigest.fromBytes(ByteBuffer.wrap(quantileSummaryCol))
    digest.quantile(0.5) should be(500.0 +- 10)
    digest.quantile(0.25) should be(250.0 +- 10)
    digest.quantile(0.75) should be(750.0 +- 10)
    digest.quantile(0.0) should be(1.0 +- 10)
    digest.quantile(1.0) should be(1000.0 +- 10)
  }
}
