package com.thetradedesk.kongming.datasets

final case class CampaignConversionReportingColumnRecord(AdvertiserId: String,
                                                         CampaignId: String,
                                                         ReportingColumnId: Int,
                                                         TrackingTagId: String,
                                                         CrossDeviceAttributionModelId: Option[String],
                                                         IncludeInCustomCPA: Boolean,
                                                         Weight: Option[BigDecimal])

object CampaignConversionReportingColumnDataSet {
  val S3Path = f"s3a://thetradedesk-useast-qubole/warehouse.external/thetradedesk.db/provisioning/campaignconversionreportingcolumn/v=1/"
}
