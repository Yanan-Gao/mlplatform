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
        SyntheticIdDensityScores: Option[Seq[SyntheticIdDensityScore]],
        SyntheticId_Level2: Option[Seq[Option[Int]]],
        SyntheticId_Level1: Option[Seq[Option[Int]]],
        split: Option[Int]
)

case class TDIDDensityScoreReadableDataset() 
  extends LightReadableDataset[TDIDDensityScoreRecord](
    s"${config.getString(s"${getClassName(TDIDDensityScoreReadableDataset)}ReadEnv", ttdEnv)}/profiles/source=bidsimpression/index=TDID/config=TDIDDensityScoreSplit/v=1/",
    FEATURE_STORE_ROOT,
    dateFormat = audienceVersionDateFormat.substring(0, 8) // Extract the first 8 characters
  )

final case class DailySeedDensityScoreRecord(
        SiteZipHashed: Option[Long],
        PopSiteZipCount: Option[Long],
        SeedId: Option[String],
        SeedSiteZipCount: Option[Long],
        SeedTotalCount: Option[Long],
        InDensity: Option[Double],
        OutDensity: Option[Double],
        DensityScore: Option[Double]
)

case class DailySeedDensityScoreReadableDataset() 
  extends LightReadableDataset[DailySeedDensityScoreRecord](
    s"${config.getString(s"${getClassName(DailySeedDensityScoreReadableDataset)}ReadEnv", ttdEnv)}/profiles/source=bidsimpression/index=SeedId/config=DailySeedDensityScore/v=1/",
    FEATURE_STORE_ROOT,
    dateFormat = audienceVersionDateFormat.substring(0, 8) // Extract the first 8 characters
  )