package com.thetradedesk.audience.jobs

import java.time.LocalDate

case class RelevanceModelOfflineScoringPart2Config(
  // TdidEmbeddingAggregate
  salt: String,
  br_emb_path: String,
  tdid_emb_path: String,
  decode_tdid: Boolean,
  // UploadEmbeddings
  emb_bucket_dest: String,
  nonsensitive_emb_enum: Int,
  sensitive_emb_enum: Int,
  base_hour: Int,
  batch_id: Int,
  // TdidEmbeddingDotProductGeneratorOOS
  seed_emb_path: String,
  density_feature_path: String,
  policy_table_path: String,
  seed_id_path: String,
  dot_product_out_path: String,
  density_split: Int,
  density_limit: Int,
  tdid_limit: Int,
  debug: Boolean,
  partition: Int,
  sensitiveModel: Boolean,
  minMaxSeedEmb: Double,
  r: Double,
  loc_factor: Double,
  // TdidSeedScoreScale
  raw_score_path: String,
  score_scale_out_path: String,
  population_score_path: String,
  smooth_factor: Double,
  userLevelUpperCap: Double,
  accuracy: Int,
  sampleRateForPercentile: Double,
  skipPercentile: Boolean,
  runDate: LocalDate,
  // TdidSeedScoreQualityCheck
  daysLookback: Int,
  percentileDiffThreshold: Double,
  seedOutlinerPercentThreshold: Double
)
