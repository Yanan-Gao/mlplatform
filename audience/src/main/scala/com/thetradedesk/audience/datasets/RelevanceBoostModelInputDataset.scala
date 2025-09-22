package com.thetradedesk.audience.datasets

import com.thetradedesk.audience.{audienceResultCoalesce, audienceVersionDateFormat, ttdWriteEnv}

final case class RelevanceBoostModelInputRecord(
                                                 BidRequestId: String,
                                                 AdvertiserId: String,
                                                 CampaignId: String,
                                                 AdGroupId: String,
                                                 IsSensitive: String,
                                                 Target: Float,
                                                 Original_NeoScore: Float,
                                                 SeedId: String,
                                                 ZipSiteLevel_Seed: Int,
                                                 FpdRelevanceMap: Map[Long, Double],
                                                 TpdRelevanceMap: Map[Long, Double],
                                                 
                                                 ExtendedFpdRelevanceMap: Map[Long, Double],
                                                 
                                                 f_MatchedTpdRelevanceTop1: Float,
                                                 f_MatchedTpdRelevanceTop2: Float,
                                                 f_MatchedTpdRelevanceTop3: Float,
                                                 f_MatchedTpdRelevanceTop4: Float,
                                                 f_MatchedTpdRelevanceTop5: Float,
                                                 f_MatchedTpdRelevanceP50: Float,
                                                 f_MatchedTpdRelevanceP90: Float,
                                                 f_MatchedTpdRelevanceSum: Float,
                                                 f_MatchedTpdRelevanceStddev: Float,
                                                 f_MatchedTpdCount: Float,
                                                 
                                                 f_MatchedFpdRelevanceTop1: Float,
                                                 f_MatchedFpdRelevanceTop2: Float,
                                                 f_MatchedFpdRelevanceTop3: Float,
                                                 f_MatchedFpdRelevanceTop4: Float,
                                                 f_MatchedFpdRelevanceTop5: Float,
                                                 f_MatchedFpdRelevanceP50: Float,
                                                 f_MatchedFpdRelevanceP90: Float,
                                                 f_MatchedFpdRelevanceSum: Float,
                                                 f_MatchedFpdRelevanceStddev: Float,
                                                 f_MatchedFpdCount: Float,

                                                //  f_MatchedFpdRelevanceP90xZipSiteLevel: Float,
                                                //  f_MatchedTpdRelevanceP90xZipSiteLevel: Float,
                                                 f_MatchedExtendedFpdRelevanceTop1: Float,
                                                 f_MatchedExtendedFpdRelevanceTop2: Float,
                                                 f_MatchedExtendedFpdRelevanceTop3: Float,
                                                 f_MatchedExtendedFpdRelevanceTop4: Float,
                                                 f_MatchedExtendedFpdRelevanceTop5: Float,
                                                 f_MatchedExtendedFpdRelevanceP50: Float,
                                                 f_MatchedExtendedFpdRelevanceP90: Float,
                                                 f_MatchedExtendedFpdRelevanceSum: Float,
                                                 f_MatchedExtendedFpdRelevanceStddev: Float,
                                                 f_MatchedExtendedFpdCount: Float,

                                                 f_MatchedFpdRelevanceP90xOriginal_NeoScore: Float,
                                                 f_MatchedTpdRelevanceP90xOriginal_NeoScore: Float,
                                                 f_MatchedExtendedFpdRelevanceP90xOriginal_NeoScore: Float,

                                                 f_Logit_NeoScore: Float
                                               )

case class RelevanceBoostModelInputDataset(tag: String, version: Int = 1) extends
  LightWritableDataset[RelevanceBoostModelInputRecord](s"/${ttdWriteEnv}/audience/RelevanceBoostModel/${tag}/v=${version}", S3Roots.ML_PLATFORM_ROOT, audienceResultCoalesce, dateFormat = audienceVersionDateFormat)
