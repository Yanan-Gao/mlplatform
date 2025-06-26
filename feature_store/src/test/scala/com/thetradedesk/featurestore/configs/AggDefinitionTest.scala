package com.thetradedesk.featurestore.configs

import com.thetradedesk.featurestore.datasets.ProfileDataset
import com.thetradedesk.featurestore.features.Features.AggFunc
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.must.Matchers.contain
import org.scalatest.matchers.should.Matchers.convertToAnyShouldWrapper
class AggDefinitionTest extends AnyFunSuite {

    test("Should load BidContextual agg config from class path successfully") {
        val aggDef = AggDefinition.loadConfig(dataSource = Some("BidContextual"))

        assertResult("/prod/features/data/contextualwithbid/v=1/date={dateStr}/hour={hourInt}/")(aggDef.prefix)

        assertResult("BidContextual")(aggDef.datasource)

        assertResult("hour")(aggDef.grain)

        val initAggDef = aggDef.extractInitialAggDefinition()
        assertResult("/prod/features/data/contextualwithbid/v=1/date={dateStr}/hour={hourInt}/")(initAggDef.prefix)

        var overridesMap = Map(
            "sourcePartition" -> "contextualwithbid",
            "jobName" -> "InitialAggJob",
            "indexPartition" -> "UIID",
            "dateStr" -> "20250603",
            "ttdEnv" -> "test",
            "hourInt" -> "1"
        )
        val outputDataSet = ProfileDataset(rootPath = initAggDef.outputRootPath, prefix = initAggDef.initOutputPrefix, grain = Some(initAggDef.grain), overrides = overridesMap)
        assertResult("s3a://thetradedesk-mlplatform-us-east-1/features/feature_store/test/profiles/source=contextualwithbid/index=UIID/job=InitialAggJob/v=1/date=20250603/hour=1")(outputDataSet.datasetPath)
    }

    test("Should load Agg Test config from file path successfully") {
        val resourcePath = "/jobconfigs/AggTestJobConfig.yml"
        val resourceUrl = getClass.getResource(resourcePath)
        var filePath = ""

        if (resourceUrl != null) {
            filePath = new java.io.File(resourceUrl.toURI).getAbsolutePath
            println(s"File path: $filePath")
        } else {
            println(s"Resource not found at $resourcePath")
        }

        val aggDef = AggDefinition.loadConfig(filePath = Some(filePath))

        assertResult("/prefix/date={dateStr}/hour={hourInt}/")(aggDef.prefix)
    }

    test("Should extract base filed agg spec successfully") {
        val aggSpec = FieldAggSpec(
            field = "FiledSum",
            dataType = "int",
            aggWindows = Seq(1, 2),
            windowUnit = "day",
            topN = 15,
            aggFuncs = Seq(AggFunc.Sum, AggFunc.Mean, AggFunc.TopN, AggFunc.Frequency, AggFunc.Min)
        )

        val baseAggSpec = aggSpec.extractFieldAggSpec("hour")
        assertResult("FiledSum")(baseAggSpec.field)
        baseAggSpec.aggFuncs should contain theSameElementsAs Seq(AggFunc.Desc, AggFunc.Frequency)
        assertResult(1500)(baseAggSpec.topN)
        assertResult(Seq(1))(baseAggSpec.aggWindows)
        assertResult("hour")(baseAggSpec.windowUnit)
        assertResult("int")(baseAggSpec.dataType)
    }
}
