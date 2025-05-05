package com.thetradedesk.plutus.data.schema.campaignbackoff

import java.sql.Timestamp

// Condensed schema of Eldorado-core UnifiedRtbPlatformReportDataSet to simplify unit testing in MockData
case class RtbPlatformReportCondensedData(
                                      ReportHourUtc: Timestamp,
                                      Country: Option[String],
                                      RenderingContext: Option[String],
                                      DeviceType: Option[String],
                                      AdFormat: Option[String],
                                      CampaignId: Option[String],
                                      SupplyVendor: Option[String],
                                      BidCount: Option[Long],
                                      ImpressionCount: Option[Long],
                                      BidAmountInUSD: Option[BigDecimal],
                                      MediaCostInUSD: Option[BigDecimal],
                                      AdvertiserCostInUSD: Option[BigDecimal],
                                      PrivateContractId: Option[String],
                                      PartnerCostInUSD: Option[BigDecimal],
                                      PredictiveClearingSavingsInUSD: Option[BigDecimal],
                                      TTDMarginInUSD: Option[BigDecimal],
                                      MarketplaceId: Option[String]
                                    )