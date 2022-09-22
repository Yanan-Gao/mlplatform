package com.thetradedesk.kongming.datasets

final case class DailyNegativeSampledBidRequestRecord (
                                                        BidRequestId: String,
                                                        LogEntryTime: java.sql.Timestamp,
                                                        UIID: String,
                                                        AdGroupId: String,
                                                        CampaignId: String,
                                                        AdvertiserId: String
                                                      )

case class DailyNegativeSampledBidRequestDataSet() extends KongMingDataset[DailyNegativeSampledBidRequestRecord](
  s3DatasetPath = "dailynegativesampledbidrequest/v=1"
)