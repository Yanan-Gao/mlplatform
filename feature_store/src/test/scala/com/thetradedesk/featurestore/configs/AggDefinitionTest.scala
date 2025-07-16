package com.thetradedesk.featurestore.configs

import com.thetradedesk.featurestore.aggfunctions.AggFunctions.AggFuncV2
import com.thetradedesk.featurestore.aggfunctions.AggFunctions.AggFuncV2.QuantileSummary
import com.thetradedesk.featurestore.datasets.ProfileDataset
import com.thetradedesk.featurestore.rsm.CommonEnums.Grain
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.must.Matchers.contain
import org.scalatest.matchers.should.Matchers.convertToAnyShouldWrapper

class AggDefinitionTest extends AnyFunSuite {

  test("Should load actual BidContextual agg config from class path successfully") {
    val aggDef = AggDefinition.loadConfig(dataSource = Some("BidContextual"))

    assertResult("/prod/features/data/contextualwithbid/v=1/date={dateStr}/hour={hourInt}/")(aggDef.dataSource.prefix)

    assertResult("BidContextual")(aggDef.dataSource.name)

    val initAggDef = aggDef.extractInitialAggDefinition()
    assertResult("/prod/features/data/contextualwithbid/v=1/date={dateStr}/hour={hourInt}/")(initAggDef.dataSource.prefix)

    val overridesMap = Map(
      "sourcePartition" -> "contextualwithbid",
      "jobName" -> "InitialAggJob",
      "indexPartition" -> "UIID",
      "dateStr" -> "20250603",
      "ttdEnv" -> "test",
      "hourInt" -> "1",
      "grain" -> Grain.Hourly.toString
    )

    // correctly resolve init agg output path
    val outputDataSet = ProfileDataset(rootPath = initAggDef.initAggConfig.outputRootPath, prefix = initAggDef.initAggConfig.outputPrefix, grain = Some(Grain.Hourly), overrides = overridesMap)
    assertResult("s3a://thetradedesk-mlplatform-us-east-1/features/feature_store/test/profiles/source=contextualwithbid/index=UIID/job=HourlyInitialAggJob/v=1/date=20250603/hour=1")(outputDataSet
      .datasetPath)

    // correctly resolve rollup agg output path
    val rollupDataSet = ProfileDataset(rootPath = aggDef.rollupAggConfig.outputRootPath, prefix = aggDef.rollupAggConfig.outputPrefix, overrides = overridesMap)
    assertResult("s3a://thetradedesk-mlplatform-us-east-1/features/feature_store/test/profiles/source=contextualwithbid/index=UIID/job=RollupAggJob/v=1/date=20250603")(rollupDataSet.datasetPath)
  }

  test("Should load Agg Test config from file path successfully") {
    val resourcePath = "/jobconfigs/AggTestJobConfig.yml"

    val aggDef = loadTestConfig(resourcePath)

    assertResult("/prefix/date={dateStr}/hour={hourInt}/")(aggDef.dataSource.prefix)
  }

  test("Should extract initial field agg specs successfully") {
    val aggSpec = FieldAggSpec(
      field = "FiledSum",
      dataType = "int",
      aggWindows = Seq(1, 2),
      aggFuncs = Seq(AggFuncV2.Sum, AggFuncV2.Mean, AggFuncV2.Frequency(Array(15)), AggFuncV2.Min)
    )

    val baseAggSpec = aggSpec.extractFieldAggSpec
    assertResult("FiledSum")(baseAggSpec.field)
    // multiply 5 to during initial aggregation
    baseAggSpec.aggFuncs should contain theSameElementsAs Seq(AggFuncV2.Sum, AggFuncV2.Count, AggFuncV2.Frequency(Array(75)), AggFuncV2.Min)
    assertResult(Seq(1))(baseAggSpec.aggWindows)
    assertResult("int")(baseAggSpec.dataType)
  }

  test("Should extract initial field agg specs from desc successfully") {
    val aggSpec = FieldAggSpec(
      field = "FiledSum",
      dataType = "int",
      aggWindows = Seq(1, 2),
      aggFuncs = Seq(AggFuncV2.Desc,
        AggFuncV2.Sum,
        AggFuncV2.Min,
        AggFuncV2.Frequency(Array(-1)), AggFuncV2.Frequency(Array(-1)),
        AggFuncV2.Frequency(Array(10)),
        AggFuncV2.Percentile(Array(10)),
        AggFuncV2.Percentile(Array(50)),
      )
    )

    val baseAggSpec = aggSpec.extractFieldAggSpec
    assertResult("FiledSum")(baseAggSpec.field)
    // multiply 100 to during initial aggregation
    baseAggSpec.aggFuncs should contain theSameElementsAs Seq(AggFuncV2.Sum,
      AggFuncV2.Count,
      AggFuncV2.Min,
      AggFuncV2.Max,
      AggFuncV2.NonZeroCount,
      AggFuncV2.Frequency(Array(-1)), // -1 will override others
      QuantileSummary)
  }

  test("Should throw exception if field name conflicts with aggregation function") {

    val resourcePath = "/jobconfigs/AggTestJobConfig_ColName_Conflict.yml"

    val thrown = intercept[IllegalArgumentException] {
      loadTestConfig(resourcePath)
    }
    assert(thrown.getMessage.contains("Column name conflicts found: FieldA_Frequency"))
  }

  def loadTestConfig(resourcePath: String): AggDefinition = {
    val resourceUrl = getClass.getResource(resourcePath)
    var filePath = ""

    if (resourceUrl != null) {
      filePath = new java.io.File(resourceUrl.toURI).getAbsolutePath
      println(s"File path: $filePath")
    } else {
      throw new IllegalArgumentException(s"Resource not found at $resourceUrl")
    }

    AggDefinition.loadConfig(filePath = Some(filePath))
  }
}
