package com.thetradedesk.plutus.data.plutus.transform.campaignfloorbuffer

import com.thetradedesk.TestUtils.TTDSparkTest
import com.thetradedesk.plutus.data.mockdata.DataGenerator
import com.thetradedesk.plutus.data.transform.campaignfloorbuffer.MergeCampaignFloorBufferTransform.{automatedFloorBufferTag, experimentFloorBufferTag, mergeFloorBufferAndAdhocData}

import java.time.{LocalDate, LocalDateTime}
import com.thetradedesk.spark.TTDSparkContext.spark.implicits._

class MergeCampaignFloorBufferTransformTest extends TTDSparkTest {

  test("Testing mergeBufferFloorAndAdhocData") {
    val todaysCampaignFloorBufferData = DataGenerator.generateCampaignFloorBufferData
    val adhocFloorBufferData = DataGenerator.generateAdhocCampaignFloorBufferData
    val res = mergeFloorBufferAndAdhocData(todaysCampaignFloorBuffer=todaysCampaignFloorBufferData,
      todaysAdhocCampaignFloorBuffer=adhocFloorBufferData)

    // Test size of the merged dataset
    val test1 = res.collectAsList()
    assert(test1.size() == 5)

    // Test campaign from experiment gets merged as expected
    val test2 = res.filter($"CampaignId" === "xyz234").collect().head
    assert(test2.AddedDate == LocalDate.of(2025, 5, 7))
    assert(test2.BufferType == experimentFloorBufferTag)
    assert(test2.BBF_FloorBuffer == 0.30)

    // Test common campaign gets updated as expected
    val test3 = res.filter($"CampaignId" === "abc123").collect().head
    assert(test3.AddedDate == LocalDate.of(2025, 5, 8))
    // Buffer type should be automated if the camapign is also selected in other criteria
    assert(test3.BufferType == automatedFloorBufferTag)
    // Test that other criteria campaigns are not getting overridden
    assert(test3.BBF_FloorBuffer == 0.01)
  }

}
