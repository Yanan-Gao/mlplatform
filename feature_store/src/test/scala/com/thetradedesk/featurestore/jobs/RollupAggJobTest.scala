package com.thetradedesk.featurestore.jobs

import com.tdunning.math.stats.MergingDigest
import com.thetradedesk.featurestore.aggfunctions.AggFunctions.AggFuncV2
import com.thetradedesk.featurestore.configs.{AggLevelConfig, FieldAggSpec}
import com.thetradedesk.featurestore.jobs.AggDefinitionHelper.createTestAggDefinition
import com.thetradedesk.featurestore.rsm.CommonEnums.Grain
import com.thetradedesk.featurestore.testutils.TTDSparkTest
import com.thetradedesk.spark.TTDSparkContext.spark.implicits._
import org.apache.spark.sql.Row
import org.scalactic.Tolerance.convertNumericToPlusOrMinusWrapper
import org.scalatest.matchers.must.Matchers.{be, contain}
import org.scalatest.matchers.should.Matchers.convertToAnyShouldWrapper

import java.nio.ByteBuffer

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
      aggFuncs = Seq(AggFuncV2.Mean, AggFuncV2.Sum, AggFuncV2.Count, AggFuncV2.Min, AggFuncV2.Max)
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
      aggFuncs = Seq(AggFuncV2.Frequency(Array(2)))
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
      aggFuncs = Seq(AggFuncV2.Frequency())
    ))

    val result = RollupAggJob.aggAndSelectByDefinition(df, aggDef, 1, aggDef.aggLevels.head).collect().toList
    result should contain theSameElementsAs Seq(
      Row("user1", Map("value1" -> 2, "value2" -> 4, "value3" -> 6, "value4" -> 8, "value5" -> 60, "value6" -> 72)),
    )
  }

  test("RollupAggJob should correctly aggregate data from base on field windows") {
    val df = Seq(
      ("user1", Map("value1" -> 1, "value2" -> 2, "value3" -> 3, "value4" -> 4, "value5" -> 5, "value6" -> 6), Map("value1" -> 11, "value2" -> 22)),
      ("user1", Map("value1" -> 1, "value2" -> 2, "value3" -> 3, "value4" -> 4, "value5" -> 55, "value6" -> 66), Map("value1" -> 11, "value2" -> 22)),
    ).toDF("FeatureKey", "valueA_Frequency", "valueB_Frequency")

    val aggDef = createTestAggDefinition(FieldAggSpec(
      field = "valueA",
      dataType = "double",
      aggWindows = Seq(1),
      aggFuncs = Seq(AggFuncV2.Frequency())
    ), FieldAggSpec(
      field = "valueB",
      dataType = "double",
      aggWindows = Seq(1, 3, 5),
      aggFuncs = Seq(AggFuncV2.Frequency())
    ))

    // agg for window 3, valueA is not included
    val result = RollupAggJob.aggAndSelectByDefinition(df, aggDef, 3, aggDef.aggLevels.head).collect().toList
    result should contain theSameElementsAs Seq(
      Row("user1", Map("value1" -> 22, "value2" -> 44)),
    )

    result.head.getAs[Map[String, Int]]("valueB_Frequency_Last3D") should contain theSameElementsAs Map("value1" -> 22, "value2" -> 44)
  }

  test("RollupAggJob should correctly aggregate vector values with Desc aggregation") {
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
      aggFuncs = Seq(AggFuncV2.Mean, AggFuncV2.Max)
    ))

    val result = RollupAggJob.aggAndSelectByDefinition(df, aggDef, 1, aggDef.aggLevels.head).collect().toList
    result should contain theSameElementsAs Seq(
      Row("user1", Seq(5, 4.5), Seq(2, 6)),
      Row("user2", Seq(2, 4), Seq(9, 12)),
    )
  }

  test("RollupAggJob should correctly aggregate quantile summary data from InitAggJob") {
    val digest = new MergingDigest(1000)
    (1 to 1000).foreach(x => digest.add(x))
    digest.compress()
    val buffer = ByteBuffer.allocate(digest.smallByteSize())
    digest.asSmallBytes(buffer)
    val bytes = buffer.array()

    val df = Seq(
      ("user1", bytes),
      ("user1", bytes)
    ).toDF("FeatureKey", "valueA_QuantileSummary")

    val aggDef = createTestAggDefinition(FieldAggSpec(
      field = "valueA",
      dataType = "double",
      aggWindows = Seq(1),
      aggFuncs = Seq(AggFuncV2.Percentile(Array(50)))
    ))

    val result = RollupAggJob.aggAndSelectByDefinition(df, aggDef, 1, aggDef.aggLevels.head).collect().toList
    val p50 = result.head.getAs[Double]("valueA_P50_Last1D")
    p50 should be(500.0 +- 1)
  }

  test("RollupAggJob should correctly aggregate and hash topn result") {
    val df = Seq(
      ("user1", Map("value1" -> 1, "value6" -> 6)),
      ("user1", Map("value1" -> 1, "value6" -> 66)),
    ).toDF("FeatureKey", "valueA_Frequency")

    val aggDef = createTestAggDefinition(FieldAggSpec(
      field = "valueA",
      dataType = "string",
      aggWindows = Seq(1),
      cardinality = Some(2),
      aggFuncs = Seq(AggFuncV2.Frequency(Array(1)))
    ))

    val result = RollupAggJob.aggAndSelectByDefinition(df, aggDef, 1, aggDef.aggLevels.head).collect().toList
    val hashValue = result.head.getAs[Seq[Int]]("valueA_Top1_Last1D_Hash")
    hashValue should be(Seq(1))
  }

  test("Rollup agg job can process AggFuncV2.Desc from init agg job") {
    val df = (0 to 1000).map(x => ("user_id", x)).toDF("user_id", "valueA")
    val aggDef = createTestAggDefinition(FieldAggSpec(
      field = "valueA",
      dataType = "double",
      aggWindows = Seq(1),
      aggFuncs = Seq(AggFuncV2.Desc)
    ))
    val initDef = aggDef.extractInitialAggDefinition()
    val initAggResult = InitialAggJob.aggByDefinition(df, initDef, AggLevelConfig(level = "user_id", saltSize = 2, initAggGrains = Array(Grain.Hourly), enableFeatureKeyCount = false)).toDF()

    val rollupAggResult = RollupAggJob.aggAndSelectByDefinition(initAggResult, aggDef, 1, aggDef.aggLevels.head).collect().toList
    val sum = rollupAggResult.head.getAs[Double]("valueA_Sum_Last1D")
    sum should be(500500)

    val p50 = rollupAggResult.head.getAs[Double]("valueA_P50_Last1D")
    p50 should be(500.0 +- 10)

    val p75 = rollupAggResult.head.getAs[Double]("valueA_P75_Last1D")
    p75 should be(750.0 +- 10)

    val mean = rollupAggResult.head.getAs[Double]("valueA_Mean_Last1D")
    mean should be(500)

    val nonZeroMean = rollupAggResult.head.getAs[Double]("valueA_NonZeroMean_Last1D")
    nonZeroMean should be(500.5)
  }

}
