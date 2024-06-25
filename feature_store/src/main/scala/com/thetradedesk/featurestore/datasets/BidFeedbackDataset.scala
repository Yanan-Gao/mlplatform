package com.thetradedesk.featurestore.datasets

final case class BidFeedbackRecord(
                                         BidRequestId: String,
                                         BidFeedbackId: String,
                                         LogEntryTime: java.sql.Timestamp,
                                         AdvertiserId: String,
                                         CampaignId: String,
                                         TDID: String,
                                         AdGroupId: String,
                                         AdvertiserCostInUSD: Option[BigDecimal],
                                         SubmittedBidAmountInUSD: Option[BigDecimal]
                                       )

case class ClickBidFeedbackRecord(
                                         BidRequestId: String,
                                         BidFeedbackId: String,
                                         LogEntryTime: java.sql.Timestamp,
                                         AdvertiserId: String,
                                         CampaignId: String,
                                         AdGroupId: String,
                                         TDID: String,
                                         AdvertiserCostInUSD: Option[BigDecimal],
                                         SubmittedBidAmountInUSD: Option[BigDecimal],
                                         ClickRedirectId: String,
                                       )


case class DailyClickBidFeedbackDataset(experimentOverride: Option[String] = None) extends ProcessedDataset[ClickBidFeedbackRecord](
  s3DatasetPath = "dailyclickbidfeedback/v=1",
  experimentOverride = experimentOverride
)