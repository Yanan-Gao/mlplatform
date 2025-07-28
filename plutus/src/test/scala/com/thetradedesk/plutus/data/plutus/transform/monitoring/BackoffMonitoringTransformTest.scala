package com.thetradedesk.plutus.data.plutus.transform.monitoring

import com.thetradedesk.TestUtils.TTDSparkTest
import com.thetradedesk.plutus.data.mockdata.MockData.{adGroupMockData, campaignUnderdeliveryForHadesMock, pcResultsLogMock, pcResultsMergedMock}
import com.thetradedesk.plutus.data.schema.{PcResultsMergedSchema, PlutusLogsData}
import com.thetradedesk.plutus.data.transform.monitoring.BackoffMonitoringTransform.{combineMetrics, getBbfOptOutRate, getHadesBufferBackoffMetrics}
import com.thetradedesk.spark.TTDSparkContext.spark.implicits._

import java.time.LocalDateTime

class BackoffMonitoringTransformTest extends TTDSparkTest {

  test("BackoffMonitoring test for schema/column correctness") {

    val campaignId = "campaign1"
    val date = LocalDateTime.of(2025, 7, 1, 12, 33).toLocalDate
    val pcOptOutData =
      Seq(
        pcResultsLogMock(bidRequestId = "abcd", bbfExceptedSource = 2),
        pcResultsLogMock(bidRequestId = "abcde", bbfExceptedSource = 0)
      ).toDS().as[PlutusLogsData]
    val adgroupData = Seq(adGroupMockData(campaignId = campaignId, adGroupId = "asdasd")).toDS()
    val pcResultsGeronimoData =
      Seq(
        pcResultsMergedMock(campaignId = Some(campaignId), maxBidMultiplierCap = 4),
        pcResultsMergedMock(campaignId = Some(campaignId), maxBidMultiplierCap = 0),
        pcResultsMergedMock(campaignId = Some(campaignId), maxBidMultiplierCap = 0),
      ).toDS().as[PcResultsMergedSchema]
    val throttleMetricDataset = Seq(campaignUnderdeliveryForHadesMock(campaignId = campaignId)).toDS()
    val hadesBufferBackoffData  = Seq((campaignId, 10, 15, 50, 0.58, 1))
      .toDF("CampaignId", "hdv3_BBF_PMP_BidCount", "hdv3_BBF_OM_BidCount", "hdv3_Total_BidCount", "CampaignBbfFloorBuffer", "pc_CampaignPCAdjustment")

    // Test buffer backoff metrics
    val hadesBufferBackoffMetrics = getHadesBufferBackoffMetrics(
      hadesBackOffData = hadesBufferBackoffData
    )
    assert(hadesBufferBackoffMetrics.count() == 1)
    val test1 = hadesBufferBackoffMetrics.filter($"CampaignId" === "campaign1").collect().head
    val (sim_BBF_OptOut_Rate, sim_BBF_PMP_OptOut_Rate, sim_BBF_OM_OptOut_Rate) = (test1.getDouble(1), test1.getDouble(2), test1.getDouble(3))
    assert(sim_BBF_OptOut_Rate == 0.5)
    assert(sim_BBF_PMP_OptOut_Rate == 0.2)
    assert(sim_BBF_OM_OptOut_Rate == 0.3)

    // Test real optout metrics data
    val finalBbfOptOutRateData = getBbfOptOutRate(
      pcResultsGeronimoData = pcResultsGeronimoData,
      adGroupData = adgroupData,
      pcOptOutData = pcOptOutData
    )
    assert(finalBbfOptOutRateData.count() == 1)
    val BBF_OptOut_Rate = finalBbfOptOutRateData.select("BBF_OptOut_Rate").head().getDouble(0)
    assert(BBF_OptOut_Rate==0.25)
    val BBF_PMP_OptOut_Rate = finalBbfOptOutRateData.select("BBF_PMP_OptOut_Rate").head().getDouble(0)
    assert(BBF_PMP_OptOut_Rate==0)
    val BBF_OM_OptOut_Rate = finalBbfOptOutRateData.select("BBF_OM_OptOut_Rate").head().getDouble(0)
    assert(BBF_OM_OptOut_Rate==0.25)
    val nonBBF_OptOut_Rate = finalBbfOptOutRateData.select("NonBBF_OptOut_Rate").head().getDouble(0)
    assert(nonBBF_OptOut_Rate==0.25)
    val maxBidMultiplierCap = finalBbfOptOutRateData.select("MaxBidMultiplierCap").head().getDouble(0)
    assert(maxBidMultiplierCap==4) // Check if aggregating the max of MaxBidCap

    // Test some of the expected metrics are present
    val finalMetricsDataset = combineMetrics(
      date = date,
      hadesBufferBackoffMetrics = hadesBufferBackoffMetrics,
      actualBbfOptoutMetrics = finalBbfOptOutRateData,
      campaignThrottleData = throttleMetricDataset
    )
    finalMetricsDataset.show()
    val test2 = finalMetricsDataset.filter($"CampaignId" === "campaign1").collect().head
    assert(test2.CampaignPCAdjustment == Some(1))
    assert(test2.CampaignBbfFloorBuffer == Some(0.58))
    assert(test2.UnderdeliveryFraction == Some(0.25))
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
    val hadesBufferBackoffMetrics  = Seq(("campaign3", 0.5, 0.2, 0.3, 0.58, 1))
      .toDF("CampaignId", "Sim_BBF_OptOut_Rate", "Sim_BBF_PMP_OptOut_Rate", "Sim_BBF_OM_OptOut_Rate", "CampaignBbfFloorBuffer", "CampaignPCAdjustment")

    val finalBbfOptOutRateData = getBbfOptOutRate(
      pcResultsGeronimoData = pcResultsGeronimoData,
      adGroupData = adgroupData,
      pcOptOutData = pcOptOutData
    )

    val hasBids_maxBidMultiplierCap = finalBbfOptOutRateData.filter($"CampaignId" === "campaign1").select("MaxBidMultiplierCap").head().getDouble(0)
    assert(hasBids_maxBidMultiplierCap == 0 && hasBids_maxBidMultiplierCap != 1.2, "Check has PcResultsGeronimo bids case: MaxBidMultiplierCap is populated from PcResultsGeronimo")
    val noBids_maxBidMultiplierCap = finalBbfOptOutRateData.filter($"CampaignId" === "campaign_OnlyOptOut").select("MaxBidMultiplierCap").head().getDouble(0)
    assert(noBids_maxBidMultiplierCap != 0 && noBids_maxBidMultiplierCap == 1.2, "Check no PcResultsGeronimo bids case: MaxBidMultiplierCap is populated from PcOptOut")

    val finalMetricsDataset = combineMetrics(
      date = date,
      hadesBufferBackoffMetrics = hadesBufferBackoffMetrics,
      actualBbfOptoutMetrics = finalBbfOptOutRateData,
      campaignThrottleData = throttleMetricDataset
    )

    val ogThrottleData = throttleMetricDataset.filter($"CampaignId" === "campaign1").collect().head
    val ogBbfOptOutRateData_IsValuePacing = finalBbfOptOutRateData.filter($"CampaignId" === "campaign1").select("IsValuePacing").head().getBoolean(0)
    val test = finalMetricsDataset.filter($"CampaignId" === "campaign1").collect().head
    assert(
      test.IsValuePacing == true &&
        ogThrottleData.IsValuePacing == false &&
        ogBbfOptOutRateData_IsValuePacing == true
      , "Check IsValuePacing mismatches between finalBbfOptOutRateData & throttleMetricDataset are properly handled by coalesce by populating value from finalBbfOptOutRateData"
    )

    val test2 = finalMetricsDataset.filter($"IsValuePacing" === true).collect()
    assert(test2.length == 2, "Expected 2 campaign in final dataset after the two left joins, instead of 4 campaigns")
  }
}
