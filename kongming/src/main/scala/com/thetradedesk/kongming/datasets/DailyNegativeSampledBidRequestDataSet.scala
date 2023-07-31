package com.thetradedesk.kongming.datasets

final case class DailyNegativeSampledBidRequestRecord (
                                                        BidRequestId: String,
                                                        LogEntryTime: java.sql.Timestamp,
                                                        UIID: String,
                                                        AdGroupId: String,
                                                        CampaignId: String,
                                                        AdvertiserId: String
                                                      )

case class DailyNegativeSampledBidRequestDataSet(experimentOverride: Option[String] = None) extends KongMingDataset[DailyNegativeSampledBidRequestRecord](
  s3DatasetPath = "dailynegativesampledbidrequest/v=1",
  experimentOverride = experimentOverride
)