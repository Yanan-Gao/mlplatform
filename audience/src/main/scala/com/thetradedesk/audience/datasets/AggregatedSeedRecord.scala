package com.thetradedesk.audience.datasets

import com.thetradedesk.audience.datasets.S3Roots.ML_PLATFORM_ROOT
import com.thetradedesk.audience.{seedCoalesceAfterFilter, ttdEnv}

final case class AggregatedSeedRecord(TDID: String,
                                      SeedIds: Seq[String],
                                      PersonGraphSeedIds: Seq[String],
                                      HouseholdGraphSeedIds: Seq[String],
                                      personId: String,
                                      householdId: String)

case class AggregatedSeedReadableDataset() extends LightReadableDataset[AggregatedSeedRecord](s"${ttdEnv}/audience/aggregatedSeed/v=1", ML_PLATFORM_ROOT)

case class AggregatedSeedWritableDataset() extends LightWritableDataset[AggregatedSeedRecord](s"${ttdEnv}/audience/aggregatedSeed/v=1", ML_PLATFORM_ROOT, 1024)
