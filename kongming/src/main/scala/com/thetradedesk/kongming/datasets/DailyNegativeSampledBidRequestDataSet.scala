package com.thetradedesk.kongming.datasets

import java.time.LocalDate

import org.apache.spark.sql.{Dataset, SaveMode}

final case class DailyNegativeSampledBidRequestRecord (
                                                        BidRequestId: String,
                                                        LogEntryTime: java.sql.Timestamp,
                                                        UIID: String,
                                                        AdGroupId: String,
                                                        CampaignId: String,
                                                        AdvertiserId: String
                                                      )

object DailyNegativeSampledBidRequestDataSet extends KongMingDataset [DailyNegativeSampledBidRequestRecord](
  s3DatasetPath = "dailynegativesampledbidrequest/v=1",
  defaultNumPartitions = 100
)