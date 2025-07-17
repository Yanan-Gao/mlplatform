package com.thetradedesk.plutus.data.plutus.transform.campaignbackoff

import com.thetradedesk.TestUtils.TTDSparkTest
import com.thetradedesk.plutus.data.mockdata.MockData.{adFormatMock}
import com.thetradedesk.plutus.data.mockdata.DataGenerator
import com.thetradedesk.plutus.data.transform.campaignbackoff.PlatformWideStatsTransform._
import com.thetradedesk.spark.datasets.sources.{AdFormatRecord}
import com.thetradedesk.spark.TTDSparkContext.spark.implicits._

class PlatformWideStatsTransformTest extends TTDSparkTest {

  test("PlatformWideStats transform test for schema/column correctness") {
    val platformReportData = DataGenerator.generatePlatformReportData // campaign comparison
    val countryData = DataGenerator.generateCountryData // join to get region

    val platformwide_agg = getPlatformWideRegionChannelStats(platformReportData, countryData)

    val res_platformwide_agg = platformwide_agg.collectAsList()
    assert(res_platformwide_agg.size() == 2, "Validating grouping")

    val group1 = res_platformwide_agg.get(0)
    val group2 = res_platformwide_agg.get(1)

    assert(group1.Avg_FirstPriceAdjustment == 0.54, "Validating average FirstPriceAdjustment of group 1")
    assert(group1.Med_FirstPriceAdjustment == 0.5, "Validating median FirstPriceAdjustment of group 1")
    assert(group1.Avg_WinRate == 0.086, "Validating average win-rate of group 1")

    assert(group2.Avg_FirstPriceAdjustment == 0.5, "Validating average FirstPriceAdjustment of group 2 (different region)")
    assert(group2.Med_FirstPriceAdjustment == 0.5, "Validating median FirstPriceAdjustment of group 2 (different region)")
    assert(group2.Avg_WinRate == 0.05, "Validating average win-rate of group 2 (different region)")
  }
}
