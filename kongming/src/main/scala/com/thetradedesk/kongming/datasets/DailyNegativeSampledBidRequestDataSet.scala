package com.thetradedesk.kongming.datasets

import com.thetradedesk.spark.util.TTDConfig.config

final case class DailyNegativeSampledBidRequestRecord (
                                                        BidRequestId: String,
                                                        LogEntryTime: java.sql.Timestamp,
                                                        UIID: String,
                                                        AdGroupId: String,
                                                        CampaignId: String,
                                                        AdvertiserId: String
                                                      )

case class DailyNegativeSampledBidRequestDataSet(experimentName: String = "") extends KongMingDataset[DailyNegativeSampledBidRequestRecord](
  s3DatasetPath = "dailynegativesampledbidrequest/v=1",
  experimentName = config.getString("ttd.DailyNegativeSampledBidRequestDataSet.experimentName", experimentName)
)