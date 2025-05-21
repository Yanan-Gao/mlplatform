package com.thetradedesk.audience.datasets

import com.thetradedesk.audience.datasets.Model.Model
import com.thetradedesk.audience.{audienceVersionDateFormat, ttdEnv, ttdWriteEnv}

case class AudienceModelThresholdRecord(SyntheticId: Int,
                                        Threshold: Float,
                                        Thresholds: Array[Float])

case class AudienceModelThresholdWritableDataset(model: Model) extends
  LightWritableDataset[AudienceModelThresholdRecord](s"configdata/${ttdWriteEnv}/audience/thresholds/${model}/v=1", "s3a://thetradedesk-mlplatform-us-east-1/", 8, dateFormat = audienceVersionDateFormat)