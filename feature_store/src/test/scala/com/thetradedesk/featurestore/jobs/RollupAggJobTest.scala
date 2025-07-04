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
      // count, nonZeroCount, sum, min, max
      ("user1", 3, 3, Some(30), Some(1), Some(11)),
      ("user1", 3, 3, Some(60), Some(2), Some(12)),
      ("user2", 2, 2, Some(70), Some(30), Some(40)),
      ("user2", 0, 0, null, null, null)
    ).toDF("FeatureKey", "valueA_Count", "valueA_NonZeroCount", "valueA_Sum", "valueA_Min", "valueA_Max")

    val aggDef = createTestAggDefinition(FieldAggSpec(
      field = "valueA",
      dataType = "double",
      aggWindows = Seq(1, 3),
      aggFuncs = Seq(AggFunc.Mean, AggFunc.Sum, AggFunc.Count, AggFunc.Min, AggFunc.Max)
    ))

    val result = RollupAggJob.aggAndSelectByDefinition(df, aggDef, 3, aggDef.aggLevels.head).collect().toList
    result should contain theSameElementsAs Seq(
      // mean, sum, count, min, max
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
      topN = 2,
      aggFuncs = Seq(AggFunc.TopN)
    ))

    val result = RollupAggJob.aggAndSelectByDefinition(df, aggDef, 1, aggDef.aggLevels.head).collect().toList
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
      aggFuncs = Seq(AggFunc.Frequency)
    ))

    val result = RollupAggJob.aggAndSelectByDefinition(df, aggDef, 1, aggDef.aggLevels.head).collect().toList
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
      aggFuncs = Seq(AggFunc.Frequency)
    ), FieldAggSpec(
      field = "valueB",
      dataType = "double",
      aggWindows = Seq(1, 3, 5),
      aggFuncs = Seq(AggFunc.Frequency)
    ))

    // agg for window 3, valueA is not included
    val result = RollupAggJob.aggAndSelectByDefinition(df, aggDef, 3, aggDef.aggLevels.head).collect().toList
    result should contain theSameElementsAs Seq(
      Row("user1", Map("value1" -> 22, "value2" -> 44)),
    )
  }

  test("InitialAggJob should correctly aggregate vector values with Desc aggregation") {
    val df = Seq(
      // count, sum, min, max
      ("user1", 2, Seq(16, 12), Seq(1, 4), Seq(2, 5)),
      ("user1", 2, Seq(4, 6), Seq(1, 5), Seq(1, 6)),
      ("user2", 4, Seq(8, 16), Seq(5, 8), Seq(9, 12)),
    ).toDF("FeatureKey", "valueA_Count", "valueA_Sum", "valueA_Min", "valueA_Max")

    val aggDef = createTestAggDefinition(FieldAggSpec(
      field = "valueA",
      dataType = "vector",
      aggWindows = Seq(1),
      aggFuncs = Seq(AggFunc.Mean, AggFunc.Max)
    ))

    val result = RollupAggJob.aggAndSelectByDefinition(df, aggDef, 1, aggDef.aggLevels.head).collect().toList
    result should contain theSameElementsAs Seq(
      Row("user1", Seq(5, 4.5), Seq(2, 6)),
      Row("user2", Seq(2, 4), Seq(9, 12)),
    )
  }
}
