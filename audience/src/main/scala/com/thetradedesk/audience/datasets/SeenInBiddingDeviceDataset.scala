package com.thetradedesk.audience.datasets

import com.thetradedesk.spark.datasets.core.IdentitySourcesS3DataSet
final case class SeenInBiddingV2DeviceRecord( Tdid: String,
                                  FirstPartyTargetingDataIds: Seq[Long],
                                  ThirdPartyTargetingDataIds: Seq[Long])

case class SeenInBiddingV2DeviceDataSet() extends
  LightReadableDataset[SeenInBiddingV2DeviceRecord] ("/prod/seeninbiddingdevicesevendayrollup/v=2", S3Roots.IDENTITY_ROOT)

final case class SeenInBiddingV3DeviceRecord( DeviceId: String,
                                              FirstPartyTargetingDataIds: Seq[Long],
                                              ThirdPartyTargetingDataIds: Seq[Long])

case class SeenInBiddingV3DeviceDataSet() extends
  LightReadableDataset[SeenInBiddingV3DeviceRecord] ("/prod/seeninbiddingdevice/v=3", S3Roots.IDENTITY_ROOT)