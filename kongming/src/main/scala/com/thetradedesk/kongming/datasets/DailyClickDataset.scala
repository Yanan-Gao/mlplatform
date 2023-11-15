package com.thetradedesk.kongming.datasets

final case class DailyClickRecord(
                         BidRequestId: String,
                         ClickRedirectId: String,
                         AdvertiserId: String,
                         CampaignId: String,
                         AdGroupId: String
                       )

case class DailyClickDataset(experimentOverride: Option[String] = None) extends KongMingDataset[DailyClickRecord](
  s3DatasetPath = "dailyclick/v=1",
  experimentOverride = experimentOverride
)