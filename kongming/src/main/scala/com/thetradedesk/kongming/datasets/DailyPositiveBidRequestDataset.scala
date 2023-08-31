package com.thetradedesk.kongming.datasets

final case class DailyPositiveLabelRecord(
                                           ConfigKey: String,
                                           ConfigValue: String,
                                           DataAggKey: String,
                                           DataAggValue: String,
                                           BidRequestId: String,
                                           TrackingTagId: String,
                                           UIID: String,
                                           ConversionTime: java.sql.Timestamp,
                                           LogEntryTime: java.sql.Timestamp,
                                           MonetaryValue: Option[BigDecimal],
                                           MonetaryValueCurrency: Option[String],
                                           IsImp: Boolean,
                                           IsClickWindowGreater: Boolean,
                                           IsInClickAttributionWindow: Boolean,
                                           IsInViewAttributionWindow: Boolean
                                         )

case class DailyPositiveBidRequestDataset(experimentOverride: Option[String] = None) extends KongMingDataset[DailyPositiveLabelRecord](
  s3DatasetPath = "dailypositive/v=1",
  experimentOverride = experimentOverride
)

final case class DailyPositiveCountSummaryRecord(
                                                  AdGroupId: String,
                                                  CampaignId: String,
                                                  AdvertiserId: String,
                                                  Count: BigInt,
                                                )

case class DailyPositiveCountSummaryDataset(experimentOverride: Option[String] = None) extends KongMingDataset[DailyPositiveCountSummaryRecord](
  s3DatasetPath = "dailypositivesummary/v=1",
  experimentOverride = experimentOverride
)