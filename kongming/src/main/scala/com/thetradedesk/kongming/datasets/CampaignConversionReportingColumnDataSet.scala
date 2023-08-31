package com.thetradedesk.kongming.datasets

import com.thetradedesk.spark.datasets.core.ProvisioningS3DataSet

final case class CampaignConversionReportingColumnRecord(AdvertiserId: String,
                                                         CampaignId: String,
                                                         ReportingColumnId: Int,
                                                         TrackingTagId: String,
                                                         CrossDeviceAttributionModelId: Option[String],
                                                         IncludeInCustomCPA: Boolean,
                                                         Weight: Option[BigDecimal],
                                                         IncludeInCustomROAS: Boolean,
                                                         CustomROASWeight: Option[BigDecimal],
                                                         CustomROASClickWeight: Option[BigDecimal],
                                                         CustomROASViewthroughWeight: Option[BigDecimal])

case class CampaignConversionReportingColumnDataSet() extends ProvisioningS3DataSet[CampaignConversionReportingColumnRecord](
  "campaignconversionreportingcolumn/v=1"){}
