package com.thetradedesk.featurestore

import com.thetradedesk.featurestore.features.Features._
import com.thetradedesk.spark.TTDSparkContext.spark.implicits._
import com.thetradedesk.featurestore.testutils.TTDSparkTest
import com.thetradedesk.featurestore.transform.AggregateTransform.genAggCols
import com.thetradedesk.featurestore.testutils.MockData
import com.thetradedesk.featurestore.testutils.MockData.TestInputDataSchema
import org.apache.spark.sql.functions.col


class AggregateTransformTest extends TTDSparkTest {

  val inputDf = (
    Seq.fill(5)(MockData.inputDfMock1) ++ Seq.fill(3)(MockData.inputDfMock2) ++ Seq.fill(2)(MockData.inputDfMock3)
      ++ Seq.fill(3)(MockData.inputDfMock4) ++ Seq.fill(2)(MockData.inputDfMock5)
  ).toDS().as[TestInputDataSchema]

  val aggWindow = 1
  val aggLevel = "FeatureKey"

  test("Aggregate Categorical Features to Get Top Elements") {
    val aggCatSpec = Array(
      CategoryFeatAggSpecs(aggField = "CategoricalFeat1", aggWindow = aggWindow, topN = 2, dataType = "string", cardinality = 9)
    )

    val aggCols = genAggCols(aggWindow, aggCatSpec)
    val aggResult = inputDf.groupBy(aggLevel).agg(aggCols.head, aggCols.tail: _*)

    assert(aggResult.filter($"FeatureKey" === "a").select(col(aggCatSpec(0).featureName)).first.get(0) == Seq("google.com", "yahoo.com"))
    assert(aggResult.filter($"FeatureKey" === "b").select(col(aggCatSpec(0).featureName)).first.get(0) == Seq("foxmail.com"))
  }


  test("Aggregate Seq of Categorical Features to Get Top Elements") {
    val aggSpec = Array(
      CategoryFeatAggSpecs(aggField = "ArrayCategoricalFeat1", aggWindow = aggWindow, topN = 3, dataType = "array", cardinality = 9)
    )

    val aggCols = genAggCols(aggWindow, aggSpec)
    val aggResult = inputDf.groupBy(aggLevel).agg(aggCols.head, aggCols.tail: _*)

    assert(aggResult.filter($"FeatureKey" === "a").select(col(aggSpec(0).featureName)).first.get(0) == Seq("111", "222", "333"))
    assert(aggResult.filter($"FeatureKey" === "b").select(col(aggSpec(0).featureName)).first.get(0) == Seq("999", "123", "111"))
  }


  test("Aggregate Numeric Features to Get Mean Values") {
    val aggSpec = Array(
      ContinuousFeatAggSpecs(aggField = "IntFeat1", aggWindow = aggWindow, aggFunc = AggFunc.Mean)
    )

    val aggCols = genAggCols(aggWindow, aggSpec)
    val aggResult = inputDf.groupBy(aggLevel).agg(aggCols.head, aggCols.tail: _*)

    assert(aggResult.filter($"FeatureKey" === "a").select(col(aggSpec(0).featureName)).first.get(0) == 4.0)
    assert(aggResult.filter($"FeatureKey" === "b").select(col(aggSpec(0).featureName)).first.get(0) == 3.2)
  }


  test("Aggregate Numeric Features to Get Average Values (exclude 0 values)") {
    val aggSpec = Array(
      ContinuousFeatAggSpecs(aggField = "FloatFeat1", aggWindow = aggWindow, aggFunc = AggFunc.Avg)
    )

    val aggCols = genAggCols(aggWindow, aggSpec)
    val aggResult = inputDf.groupBy(aggLevel).agg(aggCols.head, aggCols.tail: _*)

    assert(aggResult.filter($"FeatureKey" === "a").select(col(aggSpec(0).featureName)).first.get(0) == 3.5)
    assert(aggResult.filter($"FeatureKey" === "b").select(col(aggSpec(0).featureName)).first.get(0) == 2.5)
  }


  test("Calculate Ratio Metrics Based on Two Columns") {
    val aggSpec = Array(
      RatioFeatAggSpecs(aggField = "FloatFeat1", aggWindow = aggWindow, denomField = "IntFeat1", ratioMetrics = "Rate")
    )

    val aggCols = genAggCols(aggWindow, aggSpec)
    val aggResult = inputDf.groupBy(aggLevel).agg(aggCols.head, aggCols.tail: _*)

    assert(aggResult.filter($"FeatureKey" === "a").select(col(aggSpec(0).featureName)).first.get(0) == 0.9375)
    assert(aggResult.filter($"FeatureKey" === "b").select(col(aggSpec(0).featureName)).first.get(0) == 0.46875)
  }

  test("Aggregate Multiple Types of AggSpecs") {
    val aggSpec1 = Array(
        CategoryFeatAggSpecs(aggField = "CategoricalFeat1", aggWindow = aggWindow, topN = 2, dataType = "string", cardinality = 9)
    )
    val aggSpec2 = Array(
      ContinuousFeatAggSpecs(aggField = "FloatFeat1", aggWindow = aggWindow, aggFunc = AggFunc.Avg)
    )
    val aggSpec3 = Array(
        RatioFeatAggSpecs(aggField = "FloatFeat1", aggWindow = aggWindow, denomField = "IntFeat1", ratioMetrics = "Rate")
    )

    val aggCols = genAggCols(aggWindow, aggSpec1) ++ genAggCols(aggWindow, aggSpec2) ++ genAggCols(aggWindow, aggSpec3)
    val aggResult = inputDf.groupBy(aggLevel).agg(aggCols.head, aggCols.tail: _*)

    assert(aggResult.filter($"FeatureKey" === "a").select(col(aggSpec1(0).featureName)).first.get(0) == Seq("google.com", "yahoo.com"))
    assert(aggResult.filter($"FeatureKey" === "a").select(col(aggSpec2(0).featureName)).first.get(0) == 3.5)
    assert(aggResult.filter($"FeatureKey" === "b").select(col(aggSpec3(0).featureName)).first.get(0) == 0.46875)
  }




}
