package com.thetradedesk.plutus.data.plutus.transform.campaignbackoff

import com.thetradedesk.TestUtils.TTDSparkTest
import com.thetradedesk.plutus.data.mockdata.DataGenerator
import com.thetradedesk.plutus.data.mockdata.MockData.{adFormatMock, pcResultsMergedMock, platformWideStatsMock}
import com.thetradedesk.plutus.data.schema.PcResultsMergedDataset
import com.thetradedesk.plutus.data.schema.campaignbackoff._
import com.thetradedesk.plutus.data.transform.campaignbackoff.CampaignAdjustmentsTransform._
import com.thetradedesk.plutus.data.generateDataPathsDaily
import com.thetradedesk.spark.TTDSparkContext.spark.implicits._
import com.thetradedesk.spark.datasets.sources.AdFormatRecord
import com.thetradedesk.spark.sql.SQLFunctions.DataSetExtensions
import org.apache.spark.sql.functions.{col, to_date}
import org.apache.spark.sql.types.DecimalType
import org.scalatest.matchers.must.Matchers.contain
import org.scalatest.matchers.should.Matchers.convertToAnyShouldWrapper

import java.time.format.DateTimeFormatter
import java.time.{LocalDate, LocalDateTime}
import scala.collection.convert.ImplicitConversions.`list asScalaBuffer`
import scala.jdk.CollectionConverters.asScalaBufferConverter

class CampaignAdjustmentsTransformTest extends TTDSparkTest {
  test("Test CampaignUnderdeliveryDataset path generators") {
    val formatter = DateTimeFormatter.ofPattern("yyyy-MM-dd")
    val date = LocalDate.parse("2024-06-25", formatter)
    val expected = Seq(
      "s3://thetradedesk-mlplatform-us-east-1/model_monitor/mission_control/dev/aggregate-pacing-statistics/v=1/metric=throttle_metric_campaign_parquet/date=20240625",
      "s3://thetradedesk-mlplatform-us-east-1/model_monitor/mission_control/dev/aggregate-pacing-statistics/v=1/metric=throttle_metric_campaign_parquet/date=20240624",
      "s3://thetradedesk-mlplatform-us-east-1/model_monitor/mission_control/dev/aggregate-pacing-statistics/v=1/metric=throttle_metric_campaign_parquet/date=20240623",
      "s3://thetradedesk-mlplatform-us-east-1/model_monitor/mission_control/dev/aggregate-pacing-statistics/v=1/metric=throttle_metric_campaign_parquet/date=20240622",
      "s3://thetradedesk-mlplatform-us-east-1/model_monitor/mission_control/dev/aggregate-pacing-statistics/v=1/metric=throttle_metric_campaign_parquet/date=20240621"
    )
    val result = generateDataPathsDaily(
      CampaignThrottleMetricDataset.S3PATH,
      CampaignThrottleMetricDataset.S3PATH_DATE_GEN,
      date,
      4)

    expected should contain theSameElementsAs result
  }

  test("CampaignAdjustmentsData transform test (simple) for schema/column correctness") {
    val get_campaignUnderdeliveryData = DataGenerator.generateCampaignUnderdeliveryData
    val campaignUnderdeliveryData = get_campaignUnderdeliveryData
      .withColumn("Date", to_date(col("Date")))
      .selectAs[CampaignThrottleMetricSchema]

    val campaignFlightData = DataGenerator.generateCampaignFlightData

    /*
     *
     PART 1 getUnderdeliveringCampaigns
     *
     */

    val formatter = DateTimeFormatter.ofPattern("yyyy-MM-dd")
    val date = LocalDate.parse("2024-06-25", formatter)
    val (campaignInfo2days, underdeliveringCampaigns) = getUnderdelivery(campaignUnderdeliveryData, campaignFlightData, date)

    val res_campaignInfo = campaignInfo2days.collectAsList()
    assert(res_campaignInfo.size() == 42, "Underdelivering campaign data output rows") // all campaigns including those that are not underdelivering (excludes 1 day when campaign flight changes)

    val expectedCampaignIds = Set("newcampaign1", "newcampaign2", "newcampaign3", "newcampaign4", "newcampaign5")
    val actualCampaignIds = res_campaignInfo.asScala.map(_.getAs[String]("CampaignId")).toSet
    assert(expectedCampaignIds.subsetOf(actualCampaignIds), "Validating getting underdelivering campaigns logic and capturing those campaigns")

    /*
     *
     PART 2 getUnderdeliveringDueToPlutus
     *
     */

    // use for campaign and platform-wide stats comparison
    val platformReportData = DataGenerator.generatePlatformReportData // campaign comparison
    val countryData = DataGenerator.generateCountryData // join to get region
    val adFormatData = Seq(adFormatMock.copy()).toDS().as[AdFormatRecord] // join to get channel
    val platformWideStatsData = Seq(platformWideStatsMock.copy()).toDS().as[PlatformWideStatsSchema]

    // use for pc and discrepancy comparison --> create separate pcresultsmerged data for all 5 new campaigns that are underdelivering
    val pcResultsMergedData1 = Seq(pcResultsMergedMock(campaignId = Some("newcampaign1"))).toDS().as[PcResultsMergedDataset]
    val pcResultsMergedData2 = Seq(pcResultsMergedMock(campaignId = Some("newcampaign2"))).toDS().as[PcResultsMergedDataset]
    val pcResultsMergedData3 = Seq(pcResultsMergedMock(campaignId = Some("newcampaign3"))).toDS().as[PcResultsMergedDataset]
    val pcResultsMergedData4 = Seq(pcResultsMergedMock(campaignId = Some("newcampaign4"))).toDS().as[PcResultsMergedDataset]
    val pcResultsMergedData5 = Seq(pcResultsMergedMock(campaignId = Some("newcampaign5"))).toDS().as[PcResultsMergedDataset]
    val pcResultsMergedData6 = Seq(pcResultsMergedMock(campaignId = Some("newcampaign6"))).toDS().as[PcResultsMergedDataset]
    val pcResultsMergedDataold1 = Seq(pcResultsMergedMock(campaignId = Some("campaign4"))).toDS().as[PcResultsMergedDataset]
    val pcResultsMergedDataold2 = Seq(pcResultsMergedMock(campaignId = Some("campaign2"))).toDS().as[PcResultsMergedDataset]
    val pcResultsMergedDataold3 = Seq(pcResultsMergedMock(campaignId = Some("campaign5"))).toDS().as[PcResultsMergedDataset]
    val pcResultsMergedDataold4 = Seq(pcResultsMergedMock(campaignId = Some("ccampaign4"))).toDS().as[PcResultsMergedDataset]
    val pcResultsMergedDataold5 = Seq(pcResultsMergedMock(campaignId = Some("ccampaign5"))).toDS().as[PcResultsMergedDataset]
    val pcResultsMergedData = pcResultsMergedData1
      .union(pcResultsMergedData2)
      .union(pcResultsMergedData3)
      .union(pcResultsMergedData4)
      .union(pcResultsMergedData5)
      .union(pcResultsMergedData6)
      .union(pcResultsMergedDataold1)
      .union(pcResultsMergedDataold2)
      .union(pcResultsMergedDataold3)
      .union(pcResultsMergedDataold4)
      .union(pcResultsMergedDataold5)

    val pc_underdeliveringCampaigns = getUnderdeliveringDueToPlutus(underdeliveringCampaigns, platformReportData, countryData, adFormatData, platformWideStatsData, pcResultsMergedData)

    val res_pcUnderdeliveringCampaigns = pc_underdeliveringCampaigns.collectAsList()

    val expectedResults = Set(("newcampaign1"), ("newcampaign2"), ("newcampaign3"), ("newcampaign4"), ("campaign4"), ("ccampaign4"), ("campaign5"), ("ccampaign5"))
    val actualResults = res_pcUnderdeliveringCampaigns.asScala.map(row => (row.getAs[String]("CampaignId"))).toSet//, row.getAs[Boolean]("IsValuePacing"))).toSet
    assert(expectedResults == actualResults, "Validating checking PC underdelivering campaigns logic and capturing only PC underdelivering campaigns")

    /*
     *
     PART 3 updateAdjustments
     *
     */

    val timeformatter = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss")
    val changedEndDate_dateTime = LocalDateTime.parse("2024-09-01 00:00:00", timeformatter)

    val campaignAdjustmentsData = DataGenerator.generateCampaignAdjustmentsPacingData

    val finalCampaignAdjustments = updateAdjustments(campaignInfo2days, date, pc_underdeliveringCampaigns, campaignAdjustmentsData, testSplit = Some(0.5), updateAdjustmentsVersion = "simple")

    val res_finalCampaignAdjustments = finalCampaignAdjustments.orderBy(col("CampaignId")).collectAsList()
//    for (i <- 0 until res_finalCampaignAdjustments.size()) {
//      if (i == 0) {
//        println("finalCampaignAdjustments results:")
//      }
//      println(res_finalCampaignAdjustments.get(i))
//    }

    // Check existing campaigns in adjustments file
    val res_campaign1 = res_finalCampaignAdjustments.get(0)
    assert(res_campaign1.Pacing == Some(2) && res_campaign1.CampaignPCAdjustment == 0.75, "Validate pacing campaign counter")

    val res_campaign2 = res_finalCampaignAdjustments.get(1)
    assert(res_campaign2.AddedDate == None && res_campaign2.Pacing == Some(2) && res_campaign2.CampaignPCAdjustment == 1, "Validate flight just ended and resetting adjustment and new enddate") // AddedDate changed to null, but Pacing status will stay for that day

    val res_campaign3 = res_finalCampaignAdjustments.get(2)
    //println(res_campaign3.getClass)
    val campaign3_enddate: LocalDateTime = res_campaign3.EndDateExclusiveUTC.toLocalDateTime
    assert(res_campaign3.ImprovedNotPacing == Some(2) && res_campaign3.CampaignPCAdjustment == 0.75 && campaign3_enddate == changedEndDate_dateTime, "Validate improved-not-pacing campaign counter (major) and mid-flight changed EndDate")

    val res_campaign4 = res_finalCampaignAdjustments.get(3)
    assert(res_campaign4.WorseNotPacing == Some(2) && res_campaign4.CampaignPCAdjustment == 0.65, "Validate worse-not-pacing campaign counter and adjustment stepSize back-off")

    val res_campaign5 = res_finalCampaignAdjustments.get(4)
    assert(res_campaign5.WorseNotPacing == Some(2) && res_campaign5.CampaignPCAdjustment == 0.6, "Validate worse-not-pacing campaign counter and adjustment aggressive stepSize back-off when EndDate close")

    val res_campaign6 = res_finalCampaignAdjustments.get(5)
    assert(res_campaign6.WorseNotPacing == Some(4) && res_campaign6.CampaignPCAdjustment == 1, "Validate worse-not-pacing campaign counter when worse-not-pacing days max cap is hit and adjustment resets")

    val res_campaign7 = res_finalCampaignAdjustments.get(6)
    assert(res_campaign7.WorseNotPacing == Some(2) && res_campaign7.CampaignPCAdjustment == 0.2, "Validate when minimum adjustment back-off is hit and pacing percent change calculation when previous day's pacing fraction = 0")

    val res_campaign8 = res_finalCampaignAdjustments.get(7)
    assert(res_campaign8.AddedDate == None && res_campaign8.CampaignPCAdjustment == 1.0 && res_campaign8.Pacing == None && res_campaign8.ImprovedNotPacing == None && res_campaign8.WorseNotPacing == None, "Validate when campaign was previously set to 1")

    val res_campaign9 = res_finalCampaignAdjustments.get(8)
    assert(res_campaign9.ImprovedNotPacing == Some(2) && res_campaign9.CampaignPCAdjustment == 0.75, "Validate simple logic: improved-not-pacing campaign counter (minor) and adjustment stays the same")

    val res_campaign10 = res_finalCampaignAdjustments.get(9)
    assert(res_campaign10.ImprovedNotPacing == Some(2) && res_campaign10.CampaignPCAdjustment == 0.75, "Validate simple logic: improved-not-pacing campaign counter (minor) and adjustment stays the same")

    val res_campaign11 = res_finalCampaignAdjustments.get(10)
    assert(res_campaign11.WorseNotPacing == Some(1) && res_campaign11.CampaignPCAdjustment == 0.65, "Validate simple logic: when worse-not-pacing campaign counter hit after improving-not-pacing and adjustment stepSize back-off")

    // Check control campaigns settings
    val records_controlcampaigns = Seq(
      res_finalCampaignAdjustments.takeRight(11).head,
      res_finalCampaignAdjustments.takeRight(10).head,
      res_finalCampaignAdjustments.takeRight(9).head,
      res_finalCampaignAdjustments.takeRight(8).head,
      res_finalCampaignAdjustments.takeRight(7).head,
    )

//    val isControlCount = records_controlcampaigns.count(_.IsTest.contains(false))
//    val isDefault = records_controlcampaigns.count(_.CampaignPCAdjustment === 1.0)
//    assert(isControlCount == 5 && isDefault == 5, "Validate that there are 5 control campaigns that have default adjustment set")


    // Check newly added campaigns to adjustments file
    val res_campaignnew1 = res_finalCampaignAdjustments.takeRight(6).head
    assert(res_campaignnew1.Pacing == Some(0) && res_campaignnew1.ImprovedNotPacing == Some(0) && res_campaignnew1.WorseNotPacing == Some(0) && res_campaignnew1.CampaignPCAdjustment == 0.9, "Validate new campaign added to adjustment file - pacing/not-pacing counters set to 0 and first adjustment stepSize back-off to 0.9. Check isTest.")

    val records_newcampaigns = Seq(
      res_finalCampaignAdjustments.takeRight(6).head,
      res_finalCampaignAdjustments.takeRight(5).head,
      res_finalCampaignAdjustments.takeRight(4).head,
      res_finalCampaignAdjustments.takeRight(3).head,
    )

//    val isTestCount = records_newcampaigns.count(_.IsTest.contains(false))
//    assert(isTestCount >= 1 && isTestCount <= 3, "Validate that 4 new campaigns are split for test and control")
//
//    // Check campaigns not underdelivering due to Plutus still being recorded, but not split for test and control
//    val records_newcampaigns_notUD = Seq(
//      res_finalCampaignAdjustments.takeRight(2).head,
//      res_finalCampaignAdjustments.takeRight(1).head
//    )
//
//    val isTestCount2 = records_newcampaigns_notUD.count(_.IsTest == None)
//    assert(isTestCount2 == 2, "Validate that 2 new campaigns that are not underdelivering due to Plutus are not being split for test and control.")
  }

  test("CampaignAdjustmentsData transform test (complex & testSplit=None) for schema/column correctness") {
    val get_campaignUnderdeliveryData = DataGenerator.generateCampaignUnderdeliveryData
    val campaignUnderdeliveryData = get_campaignUnderdeliveryData
      .withColumn("Date", to_date(col("Date")))
      .selectAs[CampaignThrottleMetricSchema]

    val campaignFlightData = DataGenerator.generateCampaignFlightData

    val formatter = DateTimeFormatter.ofPattern("yyyy-MM-dd")
    val date = LocalDate.parse("2024-06-25", formatter)

    /*
     *
     PART 1 getUnderdelivery
     *
     */

    val (campaignInfo2days, underdeliveringCampaigns) = getUnderdelivery(campaignUnderdeliveryData, campaignFlightData, date)

    /*
     *
     PART 2 getUnderdeliveringDueToPlutus
     *
     */

    // use for campaign and platform-wide stats comparison
    val platformReportData = DataGenerator.generatePlatformReportData // campaign comparison
    val countryData = DataGenerator.generateCountryData // join to get region
    val adFormatData = Seq(adFormatMock.copy()).toDS().as[AdFormatRecord] // join to get channel
    val platformWideStatsData = Seq(platformWideStatsMock.copy()).toDS().as[PlatformWideStatsSchema]

    // use for pc and discrepancy comparison --> create separate pcresultsmerged data for all 5 new campaigns that are underdelivering
    val pcResultsMergedData1 = Seq(pcResultsMergedMock(campaignId = Some("newcampaign1"))).toDS().as[PcResultsMergedDataset]
    val pcResultsMergedData2 = Seq(pcResultsMergedMock(campaignId = Some("newcampaign2"))).toDS().as[PcResultsMergedDataset]
    val pcResultsMergedData3 = Seq(pcResultsMergedMock(campaignId = Some("newcampaign3"))).toDS().as[PcResultsMergedDataset]
    val pcResultsMergedData4 = Seq(pcResultsMergedMock(campaignId = Some("newcampaign4"))).toDS().as[PcResultsMergedDataset]
    val pcResultsMergedData5 = Seq(pcResultsMergedMock(campaignId = Some("newcampaign5"))).toDS().as[PcResultsMergedDataset]
    val pcResultsMergedData = pcResultsMergedData1
      .union(pcResultsMergedData2)
      .union(pcResultsMergedData3)
      .union(pcResultsMergedData4)
      .union(pcResultsMergedData5)

    val pc_underdeliveringCampaigns = getUnderdeliveringDueToPlutus(underdeliveringCampaigns, platformReportData, countryData, adFormatData, platformWideStatsData, pcResultsMergedData)

    /*
     *
     PART 3 updateAdjustments
     *
     */

    val timeformatter = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss")
    val changedEndDate_dateTime = LocalDateTime.parse("2024-09-01 00:00:00", timeformatter)

    val campaignAdjustmentsData = DataGenerator.generateCampaignAdjustmentsPacingData

    val finalCampaignAdjustments = updateAdjustments(campaignInfo2days, date, pc_underdeliveringCampaigns, campaignAdjustmentsData, testSplit = Some(1), updateAdjustmentsVersion = "complex")
    val res_finalCampaignAdjustments = finalCampaignAdjustments.orderBy(col("CampaignId")).collectAsList()

    // Check existing campaigns in adjustments file
    val res_campaign1 = res_finalCampaignAdjustments.get(0)
    assert(res_campaign1.Pacing == Some(2) && res_campaign1.CampaignPCAdjustment == 0.75, "Validate pacing campaign counter")

    val res_campaign2 = res_finalCampaignAdjustments.get(1)
    assert(res_campaign2.AddedDate == None && res_campaign2.Pacing == Some(2) && res_campaign2.CampaignPCAdjustment == 1, "Validate flight just ended and resetting adjustment and new enddate") // AddedDate changed to null, but Pacing status will stay for that day

    val res_campaign3 = res_finalCampaignAdjustments.get(2)
    //println(res_campaign3.getClass)
    val campaign3_enddate: LocalDateTime = res_campaign3.EndDateExclusiveUTC.toLocalDateTime
    assert(res_campaign3.ImprovedNotPacing == Some(2) && res_campaign3.CampaignPCAdjustment == 0.75 && campaign3_enddate == changedEndDate_dateTime, "Validate improved-not-pacing campaign counter (major) and mid-flight changed EndDate")

    val res_campaign4 = res_finalCampaignAdjustments.get(3)
    assert(res_campaign4.WorseNotPacing == Some(2) && res_campaign4.CampaignPCAdjustment == 0.65, "Validate worse-not-pacing campaign counter and adjustment stepSize back-off")

    val res_campaign5 = res_finalCampaignAdjustments.get(4)
    assert(res_campaign5.WorseNotPacing == Some(2) && res_campaign5.CampaignPCAdjustment == 0.6, "Validate worse-not-pacing campaign counter and adjustment aggressive stepSize back-off when EndDate close")

    val res_campaign6 = res_finalCampaignAdjustments.get(5)
    assert(res_campaign6.WorseNotPacing == Some(4) && res_campaign6.CampaignPCAdjustment == 1, "Validate worse-not-pacing campaign counter when worse-not-pacing days max cap is hit and adjustment resets")

    val res_campaign7 = res_finalCampaignAdjustments.get(6)
    assert(res_campaign7.WorseNotPacing == Some(2) && res_campaign7.CampaignPCAdjustment == 0.2, "Validate when minimum adjustment back-off is hit and pacing percent change calculation when previous day's pacing fraction = 0")

    val res_campaign8 = res_finalCampaignAdjustments.get(7)
    assert(res_campaign8.AddedDate == None && res_campaign8.CampaignPCAdjustment == 1.0 && res_campaign8.Pacing == None && res_campaign8.ImprovedNotPacing == None && res_campaign8.WorseNotPacing == None, "Validate when campaign was previously set to 1")

    // DIFFERENT COMPLEX LOGIC SPECIFICALLY

    val res_campaign9 = res_finalCampaignAdjustments.get(8)
    assert(res_campaign9.ImprovedNotPacing == Some(2) && res_campaign9.CampaignPCAdjustment == 0.65, "Validate complex logic: improved-not-pacing campaign counter (minor) and adjustment stepSize back-off")

    val res_campaign10 = res_finalCampaignAdjustments.get(9)
    assert(res_campaign10.ImprovedNotPacing == Some(2) && res_campaign10.CampaignPCAdjustment == 0.6, "Validate complex logic: improved-not-pacing campaign counter (minor) and adjustment aggressive stepSize back-off when EndDate close")

    val res_campaign11 = res_finalCampaignAdjustments.get(10)
    assert(res_campaign11.WorseNotPacing == Some(1) && res_campaign11.CampaignPCAdjustment == 0.85, "Validate complex logic: when worse-not-pacing campaign counter hit after improving-not-pacing and revert adjustment by regular stepSize")

    // Check newly added campaigns to adjustments file
//    val res_campaignnew1 = res_finalCampaignAdjustments.takeRight(6).head
//    assert(res_campaignnew1.Pacing == Some(0) && res_campaignnew1.ImprovedNotPacing == Some(0) && res_campaignnew1.WorseNotPacing == Some(0) && res_campaignnew1.CampaignPCAdjustment == 0.9, "Validate new campaign added to adjustment file - pacing/not-pacing counters set to 0 and first adjustment stepSize back-off to 0.9. Check isTest.")

//    val records_newcampaigns = Seq(
//      res_finalCampaignAdjustments.takeRight(6).head,
//      res_finalCampaignAdjustments.takeRight(5).head,
//      res_finalCampaignAdjustments.takeRight(4).head,
//      res_finalCampaignAdjustments.takeRight(3).head,
//    )
//
//    val isTestCount = records_newcampaigns.count(_.IsTest.contains(true))
//    assert(isTestCount == 4, "Validate that 4 new campaigns are set for test when null")

  }

}