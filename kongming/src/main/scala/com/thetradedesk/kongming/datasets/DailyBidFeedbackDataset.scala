package com.thetradedesk.kongming.datasets

final case class DailyBidFeedbackRecord(
                         BidRequestId: String,
                         BidFeedbackId: String,
                         LogEntryTime: java.sql.Timestamp,
                         AdvertiserId: String,
                         CampaignId: String,
                         AdGroupId: String,
                         AdvertiserCostInUSD: Option[BigDecimal],
                         SubmittedBidAmountInUSD: Option[BigDecimal]
                       )

case class DailyBidFeedbackDataset(experimentOverride: Option[String] = None) extends KongMingDataset[DailyBidFeedbackRecord](
  s3DatasetPath = "dailybidfeedback/v=1",
  experimentOverride = experimentOverride
  ) {
  override protected def getMetastoreTableName: String = "dailybidfeedback"
}