package com.thetradedesk.audience.jobs.customsolution

import com.thetradedesk.audience.datasets._
import java.sql.Timestamp
import java.time.LocalDate
import java.time.format.DateTimeFormatter
import org.apache.spark.sql.functions._

object BlockBlastCTVPixelGenerator {
  def main(args: Array[String]): Unit = {
    val Campaign = CampaignDataSet().readLatestPartition()
    val Partner = PartnerDataSet().readLatestPartition()
    val AdGroup = AdGroupDataSet().readLatestPartition()
    val TrackingTag = LightTrackingTagDataset().readLatestPartition()
    val CampConv = CampaignConversionReportingColumnDataset().readLatestPartition()

    val startDateLimit = expr("date_add(current_timestamp(), 2)") // 48 hours after
    val endDateLimit = expr("date_sub(current_timestamp(), 2)") // 48 hours before

    // Replicate the CTE logic
    val selectedSeed = AdGroup
      .join(Campaign, AdGroup("CampaignId") === Campaign("CampaignId"))
      .join(Partner, AdGroup("PartnerId") === Partner("PartnerId"))
      .join(CampConv, Campaign("CampaignId") === CampConv("CampaignId"))
      .filter(AdGroup("IsEnabled") && 
              Campaign("StartDate").lt(startDateLimit) && 
              (Campaign("EndDate").isNull || Campaign("EndDate").gt(endDateLimit)) &&
              !Partner("SpendDisabled") && 
              (CampConv("CrossDeviceAttributionModelId") === "IdentityAllianceWithHousehold") &&
              ((CampConv("ReportingColumnId") === 1 && Campaign("CustomCPATypeId") === 0) ||
              (CampConv("IncludeInCustomCPA") === "true" && Campaign("CustomCPATypeId") > 0)) &&
              (Campaign("PrimaryChannelId") === 4)
            )
      .select(CampConv("TrackingTagId")).distinct()
      .join(TrackingTag, Seq("TrackingTagId"))
      .filter(!lower(TrackingTag("TrackingTagName")).contains(lower(lit("test"))))
      .select(TrackingTag("TargetingDataId"), TrackingTag("TrackingTagName")).distinct()

    val dateFormat: String = "yyyyMMdd"
    val dateFormatter = DateTimeFormatter.ofPattern(dateFormat)
    val dateTime = LocalDate.now().format(dateFormatter)

    selectedSeed.write.mode("overwrite").parquet(s"s3://thetradedesk-useast-hadoop/Data_Science/Yang/audience_extension/fake_data/erm_blockblast_selected_seed/$dateTime")
  }
}

