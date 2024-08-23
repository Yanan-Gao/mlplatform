package com.thetradedesk.plutus.data.schema.campaignbackoff

// Condensed schema of Eldorado-core UnifiedRtbPlatformReportDataSet to simplify unit testing in MockData
case class RtbPlatformReportCondensedData(
                                      Country: Option[String],
                                      RenderingContext: Option[String],
                                      DeviceType: Option[String],
                                      AdFormat: Option[String],
                                      CampaignId: Option[String],
                                      BidCount: Option[Long],
                                      ImpressionCount: Option[Long],
                                      BidAmountInUSD: Option[BigDecimal],
                                      MediaCostInUSD: Option[BigDecimal],
                                      AdvertiserCostInUSD: Option[BigDecimal],
                                      PartnerCostInUSD: Option[BigDecimal],
                                      PredictiveClearingSavingsInUSD: Option[BigDecimal],
                                      TTDMarginInUSD: Option[BigDecimal]
                                    )