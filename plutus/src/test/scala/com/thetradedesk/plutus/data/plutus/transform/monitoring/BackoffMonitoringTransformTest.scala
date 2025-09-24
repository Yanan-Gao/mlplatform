package com.thetradedesk.plutus.data.plutus.transform.monitoring

import com.thetradedesk.TestUtils.TTDSparkTest
import com.thetradedesk.plutus.data.mockdata.MockData.{adGroupMockData, campaignUnderdeliveryForHadesMock, pcResultsLogMock, pcResultsMergedMock}
import com.thetradedesk.plutus.data.schema.{PcResultsMergedSchema, PlutusLogsData}
import com.thetradedesk.plutus.data.transform.monitoring.BackoffMonitoringTransform.{aggregateBidOptOutData, combineMetrics, getBackoffMetrics, unionBidAndOptOutData}
import com.thetradedesk.spark.TTDSparkContext.spark.implicits._
import com.thetradedesk.spark.sql.SQLFunctions.DataSetExtensions

import java.sql.Date
import java.time.LocalDateTime

class BackoffMonitoringTransformTest extends TTDSparkTest {

  test("BackoffMonitoring test for schema/column correctness") {

    val campaignId = "campaign1"
    val adGroupId = "asdasd"
    val date = LocalDateTime.of(2025, 7, 1, 12, 33).toLocalDate
    val pcOptOutData =
      Seq(
        pcResultsLogMock(bidRequestId = "abcd", bbfExceptedSource = 2, initialBid = 9.0, maxBidCpmInBucks = 10.0),
        pcResultsLogMock(bidRequestId = "abcde", bbfExceptedSource = 0, initialBid = 9.0, maxBidCpmInBucks = 10.0)
      ).toDS().as[PlutusLogsData]
    val adgroupData =
      Seq(
        adGroupMockData(campaignId = campaignId, adGroupId = adGroupId),
        adGroupMockData(campaignId = "campaign_pcDisabled", pcEnabled = false)
      ).toDS()
    val pcResultsGeronimoData =
      Seq(
        pcResultsMergedMock(campaignId = Some(campaignId), adgroupId = Some(adGroupId), maxBidMultiplierCap = 4, feeAmount = Some(1.0), partnerCost = Some(10.0), adjustedBidCPMInUSD = 9.0, maxBidCpmInBucks = 10.0),
        pcResultsMergedMock(campaignId = Some(campaignId), adgroupId = Some(adGroupId), maxBidMultiplierCap = 0, feeAmount = Some(0), partnerCost = Some(0), adjustedBidCPMInUSD = 9.0, maxBidCpmInBucks = 10.0),
        pcResultsMergedMock(campaignId = Some(campaignId), adgroupId = Some(adGroupId), maxBidMultiplierCap = 0, feeAmount = Some(1.0), partnerCost = Some(10.0), adjustedBidCPMInUSD = 9.0, maxBidCpmInBucks = 10.0),
        pcResultsMergedMock(campaignId = Some("campaign_pcDisabled"), pcMode = 0, model = "noPcApplied"),
        pcResultsMergedMock(campaignId = Some("campaign_usePcEnabledCoalesce"), adgroupId = Some("ag"), pcMode = 0, model = "noPcApplied"),
      ).toDS().selectAs[PcResultsMergedSchema]
    val throttleMetricDataset = Seq(campaignUnderdeliveryForHadesMock(campaignId = campaignId)).toDS()
    val backoffData  = Seq((campaignId, 10, 15, 50, 0.58, 1, 3))
      .toDF("CampaignId", "hdv3_BBF_PMP_BidCount", "hdv3_BBF_OM_BidCount", "hdv3_Total_BidCount", "CampaignBbfFloorBuffer", "MergedPCAdjustment", "VirtualMaxBid_Multiplier")

    // Test buffer backoff metrics
    val backoffMetrics = getBackoffMetrics(backoffData)

    assert(backoffMetrics.count() == 1)
    val test1 = backoffMetrics.filter($"CampaignId" === "campaign1").first()
    assert(test1.Sim_BBF_OptOut_Rate.contains(0.5))
    assert(test1.Sim_BBF_PMP_OptOut_Rate.contains(0.2))
    assert(test1.Sim_BBF_OM_OptOut_Rate.contains(0.3))

    // Test real bid metrics data
    val totalBidData = unionBidAndOptOutData(
      adGroupData = adgroupData,
      pcOptOutData = pcOptOutData,
      pcBidData = pcResultsGeronimoData
    )
    val finalBidsOptOutsMetrics = aggregateBidOptOutData(totalBidData)
    assert(finalBidsOptOutsMetrics.count() == 3)
    val test2 = finalBidsOptOutsMetrics.filter($"CampaignId" === "campaign1")
    assert(test2.select("BBF_OptOut_Rate").head().getDouble(0) == 0.25)
    assert(test2.select("BBF_PMP_OptOut_Rate").head().getDouble(0) == 0)
    assert(test2.select("BBF_OM_OptOut_Rate").head().getDouble(0) == 0.25)
    assert(test2.select("NonBBF_OptOut_Rate").head().getDouble(0) == 0.25)
    assert(test2.select("MaxBidMultiplierCap").head().getDouble(0) == 4)
    assert(test2.select("PCFeesOverSpend_Rate").head().getDouble(0) == 0.1)
    assert(test2.select("InternalBidOverMaxBid_Rate").head().getDouble(0) == 0.9)

    // Test some of the expected metrics are present
    val finalMetricsDataset = combineMetrics(
      date = date,
      backoffMetrics = backoffMetrics,
      bidOptOutMetrics = finalBidsOptOutsMetrics,
      campaignThrottleData = throttleMetricDataset
    )

//    finalMetricsDataset.show()
    val test3 = finalMetricsDataset.filter($"CampaignId" === "campaign1").collect().head
    assert(test3.CampaignPCAdjustment == Some(1))
    assert(test3.CampaignBbfFloorBuffer == Some(0.58))
    assert(test3.UnderdeliveryFraction == Some(0.25))
    assert(test3.CampaignPredictiveClearingEnabled == Some(true))

    val test_pcDisabled = finalMetricsDataset.filter($"CampaignId" === "campaign_pcDisabled").collect().head
    assert(test_pcDisabled.CampaignPredictiveClearingEnabled == Some(false))
    val test_usePcEnabledCoalesce = finalMetricsDataset.filter($"CampaignId" === "campaign_usePcEnabledCoalesce").collect().head
    assert(test_usePcEnabledCoalesce.CampaignPredictiveClearingEnabled == Some(false))
  }

  test("BackoffMonitoring Joins test for schema/column correctness") {

    val campaignId = "campaign1"
    val date = LocalDateTime.of(2025, 7, 1, 12, 33).toLocalDate
    val pcOptOutData = Seq(
      pcResultsLogMock(bidRequestId = "abcd", bbfExceptedSource = 2),
      pcResultsLogMock(bidRequestId = "abcde", bbfExceptedSource = 2, adGroupId = "adgroup_OnlyOptOut"),
    ).toDS().as[PlutusLogsData]
    val adgroupData = Seq(
      adGroupMockData(campaignId = campaignId, adGroupId = "asdasd"),
      adGroupMockData(campaignId = "campaign_OnlyOptOut", adGroupId = "adgroup_OnlyOptOut"),
    ).toDS()
    val pcResultsGeronimoData = Seq(pcResultsMergedMock(campaignId = Some(campaignId))).toDS().as[PcResultsMergedSchema]
    val throttleMetricDataset = Seq(
      campaignUnderdeliveryForHadesMock(campaignId = campaignId, isValuePacing = false),
      campaignUnderdeliveryForHadesMock(campaignId = "campaign2", isValuePacing = true)
    ).toDS()
    val backoffData  = Seq((campaignId, 10, 15, 50, 0.58, 1, 3))
      .toDF("CampaignId", "hdv3_BBF_PMP_BidCount", "hdv3_BBF_OM_BidCount", "hdv3_Total_BidCount", "CampaignBbfFloorBuffer", "MergedPCAdjustment", "VirtualMaxBid_Multiplier")

    val backoffMetrics = getBackoffMetrics(backoffData)

    // Test real bid metrics data
    val totalBidData = unionBidAndOptOutData(
      adGroupData = adgroupData,
      pcOptOutData = pcOptOutData,
      pcBidData = pcResultsGeronimoData
    )
    val finalBidsOptOutsMetrics = aggregateBidOptOutData(totalBidData)
    val finalMetricsDataset = combineMetrics(
      date = date,
      backoffMetrics = backoffMetrics,
      bidOptOutMetrics = finalBidsOptOutsMetrics,
      campaignThrottleData = throttleMetricDataset
    )

    val ogThrottleData = throttleMetricDataset.filter($"CampaignId" === "campaign1").collect().head
    val ogBbfOptOutRateData_IsValuePacing = finalBidsOptOutsMetrics.filter($"CampaignId" === "campaign1").select("IsValuePacing").head().getBoolean(0)
    val test = finalMetricsDataset.filter($"CampaignId" === "campaign1").collect().head
    assert(
      test.IsValuePacing == true &&
        ogThrottleData.IsValuePacing == false &&
        ogBbfOptOutRateData_IsValuePacing == true
      , "Check IsValuePacing mismatches between finalBbfOptOutRateData & throttleMetricDataset are properly handled by coalesce by populating value from finalBbfOptOutRateData"
    )

    val test2 = finalMetricsDataset.filter($"IsValuePacing" === true).collect()
    // "campaign2" from CampaignThrottle dataset & "campaign3" from HadesBufferBackoff dataset shouldn't be included because they don't have current bid data
    assert(test2.length == 2, "Expected 2 campaigns in final dataset after the two left joins, instead of 4 campaigns")
  }
}
