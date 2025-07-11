package com.thetradedesk.plutus.data.plutus.transform.monitoring

import com.thetradedesk.TestUtils.TTDSparkTest
import com.thetradedesk.plutus.data.mockdata.MockData.{adGroupMockData, pcResultsLogMock, pcResultsMergedMock}
import com.thetradedesk.plutus.data.schema.{PcResultsMergedSchema, PlutusLogsData}
import com.thetradedesk.plutus.data.transform.monitoring.BackoffMonitoringTransform.{combineMetrics, getBbfOptOutRate, getHadesBufferBackoffMetrics}
import com.thetradedesk.spark.TTDSparkContext.spark.implicits._

import java.time.LocalDateTime

class BackoffMonitoringTransformTest extends TTDSparkTest {

  test("BackoffMonitoring test for schema/column correctness") {

    val campaignId = "campaign1"
    val date = LocalDateTime.of(2025, 7, 1, 12, 33).toLocalDate
    val pcOptOutData = Seq(pcResultsLogMock(bidRequestId = "abcd", bbfExceptedSource = 2)).toDS().as[PlutusLogsData]
    val adgroupData = Seq(adGroupMockData(campaignId = campaignId, adGroupId = "asdasd")).toDS()
    val pcResultsGeronimoData = Seq(pcResultsMergedMock(campaignId = Some(campaignId))).toDS().as[PcResultsMergedSchema]
    val hadesBufferBackoffData  = Seq((campaignId, 10, 15, 50, 0.07, 0.58, 1))
      .toDF("CampaignId", "hdv3_BBF_PMP_BidCount", "hdv3_BBF_OM_BidCount", "hdv3_Total_BidCount", "hdv3_UnderdeliveryFraction", "CampaignBbfFloorBuffer", "pc_CampaignPCAdjustment")

    // Test buffer backoff metrics
    val hadesBufferBackoffMetrics = getHadesBufferBackoffMetrics(
      hadesBackOffData =hadesBufferBackoffData
    )
    assert(hadesBufferBackoffMetrics.count() == 1)
    val test1 = hadesBufferBackoffMetrics.filter($"CampaignId" === "campaign1").collect().head
    val (sim_BBF_OptOut_Rate, sim_BBF_PMP_OptOut_Rate) = (test1.getDouble(1), test1.getDouble(2))
    assert(sim_BBF_OptOut_Rate == 0.5)
    assert(sim_BBF_PMP_OptOut_Rate == 0.2)

    // Test real optout metrics data
    val finalBbfOptOutRateData = getBbfOptOutRate(
      date = date,
      pcResultsGeronimoData = pcResultsGeronimoData,
      adGroupData = adgroupData,
      pcOptOutData = pcOptOutData
    )

    assert(finalBbfOptOutRateData.count() == 1)
    val actual_BBF_OptOut_Rate = finalBbfOptOutRateData.select("actual_BBF_OptOut_Rate").head().getDouble(0)
    assert(actual_BBF_OptOut_Rate==0.5)

    // Test some of the expected metrics are present
    val finalMetricsDataset = combineMetrics(hadesBufferBackoffMetrics=hadesBufferBackoffMetrics,
      actualBbfOptoutMetrics = finalBbfOptOutRateData)
    val test2 = finalMetricsDataset.filter($"CampaignId" === "campaign1").collect().head
    assert(test2.CampaignPCAdjustment == 1)
    assert(test2.CampaignBbfFloorBuffer == 0.58)
  }
}
