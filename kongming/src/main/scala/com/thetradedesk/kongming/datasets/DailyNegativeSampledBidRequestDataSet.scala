package com.thetradedesk.kongming.datasets

import java.time.LocalDate

import org.apache.spark.sql.{Dataset, SaveMode}

final case class NegativeSamplingBidRequestGrainsRecord(
                                                       BidRequestId: String,
                                                       UIID: String,
                                                       AdvertiserId: String,
                                                       CampaignId: String,
                                                       AdGroupId: String,
                                                       SupplyVendor: String,
                                                       Site: String,
                                                       RenderingContext: String,
                                                       Country: String,
                                                       DeviceType: String,
                                                       OperatingSystemFamily: String,
                                                       Browser: String
                                                       )
final case class DailyNegativeSampledBidRequestRecord (
                                                        BidRequestId: String,
                                                        UIID: String,
                                                        AdGroupId: String,
                                                        CampaignId: String,
                                                        AdvertiserId: String
                                                      )

object DailyNegativeSampledBidRequestDataSet extends KongMingDataset [DailyNegativeSampledBidRequestRecord](
  s3DatasetPath = "dailynegativesampledbidrequest/v=1",
  defaultNumPartitions = 100
)
