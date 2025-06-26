package com.thetradedesk.featurestore.jobs

import com.thetradedesk.featurestore.configs.FieldAggSpec
import com.thetradedesk.featurestore.features.Features.AggFunc
import com.thetradedesk.featurestore.jobs.AggDefinitionHelper.createTestAggDefinition
import com.thetradedesk.featurestore.testutils.TTDSparkTest
import com.thetradedesk.spark.TTDSparkContext.spark.implicits._
import org.apache.spark.sql.Row
import org.scalatest.matchers.must.Matchers.contain
import org.scalatest.matchers.should.Matchers.convertToAnyShouldWrapper

class RollupAggJobTest extends TTDSparkTest {

  test("RollupAggJob should correctly aggregate description data from InitAggJob") {
    val df = Seq(
      ("user1", Map("count" -> 3, "nonZeroCount" -> 3, "sum" -> 30, "min" -> 1, "max" -> 11, "allNull" -> 0)),
      ("user1", Map("count" -> 3, "nonZeroCount" -> 3, "sum" -> 60, "min" -> 2, "max" -> 12, "allNull" -> 0)),
      ("user2", Map("count" -> 2, "nonZeroCount" -> 2, "sum" -> 70, "min" -> 30, "max" -> 40, "allNull" -> 0)),
      ("user2", Map("count" -> 2, "nonZeroCount" -> 2, "sum" -> 70, "min" -> 30, "max" -> 40, "allNull" -> 1)) // this should be ignored
    ).toDF("FeatureKey", "valueA_Desc")

    val aggDef = createTestAggDefinition(FieldAggSpec(
      field = "valueA",
      dataType = "double",
      aggWindows = Seq(1, 3),
      windowUnit = "day",
      aggFuncs = Seq(AggFunc.Mean, AggFunc.Sum, AggFunc.Count, AggFunc.Min, AggFunc.Max)
    ))

    val result = RollupAggJob.aggByDefinition(df, aggDef, 3).collect().toList
    result should contain theSameElementsAs Seq(
      Row("user1", 15, 90, 6, 1, 12),
      Row("user2", 35, 70, 2, 30, 40)
    )
  }

  test("RollupAggJob should correctly aggregate topN data from InitAggJob") {
    val df = Seq(
      ("user1", Map("value1" -> 1, "value2" -> 2, "value3" -> 3, "value4" -> 4, "value5" -> 5, "value6" -> 6)),
      ("user1", Map("value1" -> 1, "value2" -> 2, "value3" -> 3, "value4" -> 4, "value5" -> 55, "value6" -> 66)),
    ).toDF("FeatureKey", "valueA_Frequency")

    val aggDef = createTestAggDefinition(FieldAggSpec(
      field = "valueA",
      dataType = "double",
      aggWindows = Seq(1),
      windowUnit = "day",
      topN = 2,
      aggFuncs = Seq(AggFunc.TopN)
    ))

    val result = RollupAggJob.aggByDefinition(df, aggDef, 1).collect().toList
    result should contain theSameElementsAs Seq(
      Row("user1", Seq("value6", "value5")),
    )
  }

  test("RollupAggJob should correctly aggregate frequency data from InitAggJob") {
    val df = Seq(
      ("user1", Map("value1" -> 1, "value2" -> 2, "value3" -> 3, "value4" -> 4, "value5" -> 5, "value6" -> 6)),
      ("user1", Map("value1" -> 1, "value2" -> 2, "value3" -> 3, "value4" -> 4, "value5" -> 55, "value6" -> 66)),
    ).toDF("FeatureKey", "valueA_Frequency")

    val aggDef = createTestAggDefinition(FieldAggSpec(
      field = "valueA",
      dataType = "double",
      aggWindows = Seq(1),
      windowUnit = "day",
      aggFuncs = Seq(AggFunc.Frequency)
    ))

    val result = RollupAggJob.aggByDefinition(df, aggDef, 1).collect().toList
    result should contain theSameElementsAs Seq(
      Row("user1", Map("value1" -> 2, "value2" -> 4, "value3" -> 6, "value4" -> 8, "value5" -> 60, "value6" -> 72)),
    )
  }

  test("InitialAggJob should correctly aggregate data from base on field windows") {
    val df = Seq(
      ("user1", Map("value1" -> 1, "value2" -> 2, "value3" -> 3, "value4" -> 4, "value5" -> 5, "value6" -> 6), Map("value1" -> 11, "value2" -> 22)),
      ("user1", Map("value1" -> 1, "value2" -> 2, "value3" -> 3, "value4" -> 4, "value5" -> 55, "value6" -> 66), Map("value1" -> 11, "value2" -> 22)),
    ).toDF("FeatureKey", "valueA_Frequency", "valueB_Frequency")

    val aggDef = createTestAggDefinition(FieldAggSpec(
      field = "valueA",
      dataType = "double",
      aggWindows = Seq(1),
      windowUnit = "day",
      aggFuncs = Seq(AggFunc.Frequency)
    ), FieldAggSpec(
      field = "valueB",
      dataType = "double",
      aggWindows = Seq(1, 3, 5),
      windowUnit = "day",
      aggFuncs = Seq(AggFunc.Frequency)
    ))

    // agg for window 3, valueA is not included
    val result = RollupAggJob.aggByDefinition(df, aggDef, 3).collect().toList
    result should contain theSameElementsAs Seq(
      Row("user1", Map("value1" -> 22, "value2" -> 44)),
    )
  }

  test("InitialAggJob should correctly aggregate vector values with Desc aggregation") {
    val df = Seq(
      ("user1", Seq(Seq(1, 1), Seq(11, 12), Seq(11, 12), Seq(11, 12))),
      ("user2", Seq(Seq(2, 2), Seq(6, 12), Seq(1, 3), Seq(5, 9))),
    ).toDF("FeatureKey", "valueA_VectorDesc")

    val aggDef = createTestAggDefinition(FieldAggSpec(
      field = "valueA",
      dataType = "array_int",
      arraySize = 2,
      aggWindows = Seq(1),
      windowUnit = "day",
      aggFuncs = Seq(AggFunc.VectorMean, AggFunc.VectorMax)
    ))

    val result = RollupAggJob.aggByDefinition(df, aggDef, 1).collect().toList
    result should contain theSameElementsAs Seq(
      Row("user1", Seq(11, 12), Seq(11, 12)),
      Row("user2", Seq(3, 6), Seq(5, 9)),
    )
  }
}
