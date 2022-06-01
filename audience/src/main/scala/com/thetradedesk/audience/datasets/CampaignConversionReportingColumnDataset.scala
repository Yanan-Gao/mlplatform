package com.thetradedesk.audience.datasets

final case class CampaignConversionReportingColumnRecord(AdvertiserId: String,
                                                         CampaignId: String,
                                                         ReportingColumnId: Int,
                                                         TrackingTagId: String,
                                                         CrossDeviceAttributionModelId: Option[String],
                                                         IncludeInCustomCPA: Boolean,
                                                         Weight: Option[BigDecimal])

case class CampaignConversionReportingColumnDataset() extends
  LightReadableDataset[CampaignConversionReportingColumnRecord]("/warehouse.external/thetradedesk.db/provisioning/campaignconversionreportingcolumn/v=1", "s3a://thetradedesk-useast-qubole")