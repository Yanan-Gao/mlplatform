package com.thetradedesk.audience.datasets

import com.thetradedesk.audience.datasets.S3Roots.ML_PLATFORM_ROOT
import com.thetradedesk.audience.{seedCoalesceAfterFilter, ttdEnv, getClassName, audienceVersionDateFormat, audienceResultCoalesce}
import java.sql.Date
import com.thetradedesk.spark.util.TTDConfig.config

final case class RelevanceSeedsRecord(SeedIds: Array[String],
                                        SyntheticIds: Array[Int],
                                        SensitiveSeedIds: Array[String],
                                        SensitiveSyntheticIds: Array[Int],
                                        InsensitiveSeedIds: Array[String],
                                        InsensitiveSyntheticIds: Array[Int]
)

case class RelevanceSeedsDataset(version: Int = 1) extends LightWritableDataset[RelevanceSeedsRecord](s"${ttdEnv}/audience/relevanceseedsdataset/v=${version}", ML_PLATFORM_ROOT, 1)
