package com.thetradedesk.audience.datasets

import com.thetradedesk.spark.datasets.core.ProvisioningS3DataSet

final case class CampaignConversionReportingColumnRecord(AdvertiserId: String,
                                                         CampaignId: String,
                                                         ReportingColumnId: Int,
                                                         TrackingTagId: String,
                                                         CrossDeviceAttributionModelId: Option[String],
                                                         IncludeInCustomCPA: Boolean,
                                                         Weight: Option[BigDecimal])

case class CampaignConversionReportingColumnDataset() extends
  ProvisioningS3DataSet[CampaignConversionReportingColumnRecord]("campaignconversionreportingcolumn/v=1", true)