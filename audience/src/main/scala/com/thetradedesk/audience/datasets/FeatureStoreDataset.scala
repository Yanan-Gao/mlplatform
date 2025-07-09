package com.thetradedesk.audience.datasets

import com.thetradedesk.audience.datasets.S3Roots.FEATURE_STORE_ROOT
import com.thetradedesk.audience.{seedCoalesceAfterFilter, ttdEnv, getClassName, audienceVersionDateFormat}
import java.sql.Date
import com.thetradedesk.spark.util.TTDConfig.config

final case class SyntheticIdDensityScore(
  syntheticId: Option[Int],
  densityScore: Option[Float]
)
final case class TDIDDensityScoreRecord(
        TDID: Option[String],
        SyntheticId_Level2: Option[Seq[Option[Int]]],
        SyntheticId_Level1: Option[Seq[Option[Int]]],
        split: Option[Int]
)

case class TDIDDensityScoreReadableDataset()
  extends LightReadableDataset[TDIDDensityScoreRecord](
    s"${config.getString(s"${getClassName(TDIDDensityScoreReadableDataset)}ReadEnv", ttdEnv)}/profiles/source=bidsimpression/index=TDID/job=DailyTDIDDensityScoreSplitJob/v=1/",
    FEATURE_STORE_ROOT // Extract the first 8 characters
  )
final case class SeedDensityScoreRecord(
        FeatureKey: String,
        FeatureValueHashed: Long,
        SyntheticIdLevel2: Option[Seq[Option[Int]]],
        SyntheticIdLevel1: Option[Seq[Option[Int]]],
)

case class SeedDensityScoreReadableDataset() 
  extends LightReadableDataset[SeedDensityScoreRecord](
    s"${config.getString(s"${getClassName(SeedDensityScoreReadableDataset)}ReadEnv", ttdEnv)}/profiles/source=bidsimpression/index=FeatureKeyValue/job=DailyDensityScoreReIndexingJob/config=SyntheticIdDensityScoreCategorized/",
    FEATURE_STORE_ROOT,
    dateFormat = audienceVersionDateFormat.substring(0, 8) // Extract the first 8 characters
  )
final case class DailySeedDensityScoreRecord(
        FeatureKey: String,
        FeatureValueHashed: Long,
        SeedId: Option[String],
        DensityScore: Option[Double]
)

case class DailySeedDensityScoreReadableDataset() 
  extends LightReadableDataset[DailySeedDensityScoreRecord](
    s"${config.getString(s"${getClassName(DailySeedDensityScoreReadableDataset)}ReadEnv", ttdEnv)}/profiles/source=bidsimpression/index=SeedId/job=DailySeedDensityScore/v=1/",
    FEATURE_STORE_ROOT,
    dateFormat = audienceVersionDateFormat.substring(0, 8) // Extract the first 8 characters
  )