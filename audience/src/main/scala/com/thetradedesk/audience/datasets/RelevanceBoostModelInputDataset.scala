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
                                                 DataRelevanceMap: Map[Long, Double],
                                                 AdvertiserFirstPartyDataRelevanceMap: Map[Long, Double],

                                                 MatchedTpdRelevanceP50: Float,
                                                 MatchedTpdRelevanceP90: Float,
                                                 MatchedTpdRelevanceSum: Float,
                                                 MatchedTpdRelevanceStddev: Float,
                                                 MatchedTpdCount: Int,

                                                 MatchedFpdRelevanceP50: Float,
                                                 MatchedFpdRelevanceP90: Float,
                                                 MatchedFpdRelevanceSum: Float,
                                                 MatchedFpdRelevanceStddev: Float,
                                                 MatchedFpdCount: Int,

                                                 MatchedFpdRelevanceP90xZipSiteLevel: Float,
                                                 MatchedTpdRelevanceP90xZipSiteLevel: Float,
                                                 MatchedFpdRelevanceP90xOriginal_NeoScore: Float,
                                                 MatchedTpdRelevanceP90xOriginal_NeoScore: Float
                                               )

case class RelevanceBoostModelInputDataset(tag: String, version: Int = 1) extends
  LightWritableDataset[RelevanceBoostModelInputRecord](s"/${ttdWriteEnv}/audience/RelevanceBoostModel/${tag}/v=${version}", S3Roots.ML_PLATFORM_ROOT, audienceResultCoalesce, dateFormat = audienceVersionDateFormat)
