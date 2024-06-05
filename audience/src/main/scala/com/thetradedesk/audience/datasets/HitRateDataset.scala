package com.thetradedesk.audience.datasets

import com.thetradedesk.audience.datasets.S3Roots.ML_PLATFORM_ROOT
import com.thetradedesk.audience.{seedCoalesceAfterFilter, ttdEnv, getClassName}
import java.sql.Date
import com.thetradedesk.spark.util.TTDConfig.config

final case class HitRateRecord(SeedId: String,
                                      CampaignId: String,
                                      AdGroupId: String,
                                      ReportDate: Date,
                                      ImpressionCount: Long,
                                      SeedImpressionCount: Long,
                                      HitCount: Long,
                                      HitRate: Double,
                                      )

case class HitRateReadableDataset() extends LightReadableDataset[HitRateRecord](s"${config.getString(s"${getClassName(HitRateReadableDataset)}ReadEnv", ttdEnv)}/audience/measurement/hitRate/v=1", ML_PLATFORM_ROOT)

case class HitRateWritableDataset() extends LightWritableDataset[HitRateRecord](s"${ttdEnv}/audience/measurement/hitRate/v=1", ML_PLATFORM_ROOT, 1024)
