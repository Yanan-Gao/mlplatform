package com.thetradedesk.audience.datasets

import com.thetradedesk.audience.ttdEnv

final case class ExperimentEventRecord(AvailableBidRequestId: String,
                                       TDID: String,
                                       LookalikeLabel: Boolean,
                                       ModelLabel: Boolean,
                                       TrueLabel: Boolean,
                                       WasHeldOut: Boolean
                                    )

case class ExperimentEventDataset() extends
  LightWritableDataset[ExperimentEventRecord](s"/${ttdEnv}/audience/experimentEvents/v=1", S3Roots.ML_PLATFORM_ROOT, 100)