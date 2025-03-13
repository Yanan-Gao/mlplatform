package com.thetradedesk.plutus.data.plutus.transform

import com.thetradedesk.TestUtils.TTDSparkTest
import com.thetradedesk.geronimo.bidsimpression.schema.BidsImpressionsSchema
import com.thetradedesk.geronimo.shared.{intModelFeaturesCols, parseModelFeaturesFromJson}
import com.thetradedesk.plutus.data.mockdata.MockData._
import com.thetradedesk.plutus.data.schema._
import com.thetradedesk.plutus.data.transform.RawDataTransform
import com.thetradedesk.plutus.data.transform.TrainingDataTransform.{modelTargetCols, modelTargets}
import com.thetradedesk.plutus.data.{DEFAULT_SHIFT, hashedModMaxIntFeaturesCols}
import com.thetradedesk.spark.TTDSparkContext.spark.implicits._
import com.thetradedesk.spark.sql.SQLFunctions.DataSetExtensions
import org.apache.spark.sql.functions.{col, lit}
import org.apache.spark.sql.types.FloatType
import org.scalatest.matchers.should._

class ETLv1UserDataTest extends TTDSparkTest with Matchers {

  test("ETLv1 Raw and Clean processing for user-data correctness") {

    val date = java.time.LocalDate.parse("2021-12-25")


    val geronimoDataset = Seq(
      bidsImpressionsMock(SupplyVendorValue = Some("1"), SupplyVendorPublisherId = Some("1")),
      bidsImpressionsMock(SupplyVendorValue = Some("1"), SupplyVendorPublisherId = Some("2")),
      bidsImpressionsMock(SupplyVendorValue = Some("1"), SupplyVendorPublisherId = Some("3")),
      bidsImpressionsMock(SupplyVendorValue = Some("2"), SupplyVendorPublisherId = Some("3")),
      bidsImpressionsMock(SupplyVendorValue = Some("3"), SupplyVendorPublisherId = Some("3")),
    ).toDS().as[BidsImpressionsSchema]


    val svbDataset = svbMock()
    val pdaDataset = pdaMock()
    val dealsDataset = dealsMock()

    val dealsDF = RawDataTransform.dealData(svb = svbDataset, deals = dealsDataset)

    val rawTransformedData = RawDataTransform.allData(
        date = date,
        svNames = Seq("1"),
        bidsImpressions = geronimoDataset,
        svb = svbDataset,
        pda = pdaDataset,
        dealDf = dealsDF,
        empDisDf = empiricalDiscrepancyMock(),
        partitions = 1
      ).withColumn("mb2w", lit(1.23))
      .withColumn("is_imp", col("IsImp").cast(FloatType))
      .withColumn("AuctionBidPrice", col("b_RealBidPrice"))

    rawTransformedData.count should equal(3)
    rawTransformedData.columns should contain allOf("UserAgeInDays", "UserSegmentCount")


    val cleanData = rawTransformedData.selectAs[CleanInputData]
    cleanData.columns should contain allOf("UserAgeInDays", "UserSegmentCount")

  }

  test("New Feature JSON should create the correct selection query for use in ModelInput") {
    import scala.io.Source

    def loadFileAsString(filePath: String): String = {
      val source = Source.fromFile(filePath)
      try source.mkString finally source.close()
    }

    val jsonString = loadFileAsString("src/test/resources/features-user-data.json")
    val modelFeatures = parseModelFeaturesFromJson(jsonString)

    val selectionTabular = intModelFeaturesCols(modelFeatures) ++ modelTargetCols(modelTargets)
    val selectionTabMaxMod = hashedModMaxIntFeaturesCols(modelFeatures, DEFAULT_SHIFT) ++ modelTargetCols(modelTargets)

    selectionTabular.mkString("Array(", ", ", ")") should include("UserAgeInDays")
    selectionTabular.mkString("Array(", ", ", ")") should include("UserSegmentCount")
    selectionTabMaxMod.mkString("Array(", ", ", ")") should include("UserAgeInDays")
    selectionTabMaxMod.mkString("Array(", ", ", ")") should include("UserSegmentCount")

    modelFeatures.map(_.name) should contain allOf("UserAgeInDays", "UserSegmentCount")

  }
}
