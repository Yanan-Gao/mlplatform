package com.thetradedesk.featurestore.aggfunctions

import com.thetradedesk.featurestore.aggfunctions.AggFunctions.AggFuncV2
import org.scalatest.funsuite.AnyFunSuite

class AggFuncV2Test extends AnyFunSuite {

  test("Should parse and compact AggFuncV2 correctly") {
    val funcs = Seq("Sum", "Count", "Mean", "Mean", "Min", "Max", "Frequency", "Top10", "P10", "P50")
    val parsedFuncs = AggFuncV2.parseAggFuncs(funcs, "FieldA")
    assertResult(Seq(AggFuncV2.Sum, AggFuncV2.Count, AggFuncV2.Mean, AggFuncV2.Min, AggFuncV2.Max, AggFuncV2.Frequency(Array(-1, 10)), AggFuncV2.Percentile(Array(10, 50))))(parsedFuncs)
  }

  test("Should parse AggFuncV2 correctly") {
    assertResult(Some(AggFuncV2.Sum))(AggFuncV2.fromString("sum"))
    assertResult(Some(AggFuncV2.Count))(AggFuncV2.fromString("count"))
    assertResult(Some(AggFuncV2.Mean))(AggFuncV2.fromString("mean"))
    assertResult(Some(AggFuncV2.NonZeroMean))(AggFuncV2.fromString("nonzeromean"))
    assertResult(Some(AggFuncV2.Median))(AggFuncV2.fromString("median"))
    assertResult(Some(AggFuncV2.Desc))(AggFuncV2.fromString("desc"))
    assertResult(Some(AggFuncV2.NonZeroCount))(AggFuncV2.fromString("nonzerocount"))
    assertResult(Some(AggFuncV2.Min))(AggFuncV2.fromString("min"))
    assertResult(Some(AggFuncV2.Max))(AggFuncV2.fromString("max"))
    assertResult(Some(AggFuncV2.Frequency(Array(-1))))(AggFuncV2.fromString("frequency"))

    // with parameter
    assertResult(None)(AggFuncV2.fromString("topN"))
    assertResult(Some(AggFuncV2.Frequency(Array(10))))(AggFuncV2.fromString("top10"))

    assertResult(None)(AggFuncV2.fromString("P"))
    assertResult(Some(AggFuncV2.Percentile(Array(10))))(AggFuncV2.fromString("P10"))
  }

}
