package com.thetradedesk.audience.jobs

import com.thetradedesk.audience.{date, dateFormatter, ttdEnv}
import com.thetradedesk.spark.util.TTDConfig.config

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
                                                    samplingRate: Int,
                                                    // TdidSeedScoreScale
                                                    raw_score_path: String,
                                                    score_scale_out_path: String,
                                                    population_score_path: String,
                                                    full_out_path: String,
                                                    smooth_factor: Double,
                                                    userLevelUpperCap: Double,
                                                    accuracy: Int,
                                                    sampleRateForPercentile: Double,
                                                    skipPercentile: Boolean,
                                                    // TdidSeedScoreQualityCheck
                                                    daysLookback: Int,
                                                    percentileDiffThreshold: Double,
                                                    seedOutlinerPercentThreshold: Double,
                                                    // common
                                                    runDate: LocalDate
                                                  )

object RelevanceModelOfflineScoringLegacyConfigLoader {
  def loadLegacyPart2Config(): RelevanceModelOfflineScoringPart2Config = {
    val dateStr = date.format(dateFormatter)
    RelevanceModelOfflineScoringPart2Config(
      // TdidEmbeddingAggregate
      salt = "TRM",
      br_emb_path = config.getString(
        "br_emb_path", s"s3://thetradedesk-mlplatform-us-east-1/data/${ttdEnv}/audience/RSMV2/emb/raw/v=1/date=${dateStr}/"),
      tdid_emb_path = config.getString(
        "tdid_emb_path", s"s3://thetradedesk-mlplatform-us-east-1/data/${ttdEnv}/audience/RSMV2/emb/agg/v=1/date=${dateStr}/"),
      decode_tdid = config.getBoolean("decode_tdid", true), // convert binary tdid format into string
      // UploadEmbeddings
      emb_bucket_dest = ttdEnv match {
        case "prod" => config.getString("emb_bucket_dest", "s3://ttd-user-embeddings/dataexport/")
        case _ => "s3://thetradedesk-mlplatform-us-east-1/data/prodTest/audience/emb/dataexport/"
      },
      nonsensitive_emb_enum = config.getInt("nonsensitive_emb_enum", 301),
      sensitive_emb_enum = config.getInt("sensitive_emb_enum", 302),
      base_hour = config.getInt("base_hour", 0),
      batch_id = config.getInt("batch_id", 1),
      // TdidEmbeddingDotProductGeneratorOOS
      seed_emb_path = config.getString("seed_emb_path", s"s3://thetradedesk-mlplatform-us-east-1/configdata/test/audience/embedding/RSMV2/RSMv2SensitiveDensityTest/v=1/${dateStr}000000/"),
      density_feature_path = config.getString("density_feature_path", s"s3://thetradedesk-mlplatform-us-east-1/features/feature_store/prod/profiles/source=bidsimpression/index=TDID/job=DailyTDIDDensityScoreSplitJob/v=1/date=${dateStr}/"),
      policy_table_path = config.getString("policy_table_path", s"s3://thetradedesk-mlplatform-us-east-1/configdata/prod/audience/policyTable/RSM/v=1/${dateStr}000000/"),
      seed_id_path = config.getString("seed_id_path", s"s3://thetradedesk-mlplatform-us-east-1/data/${ttdEnv}/audience/scores/seedids/v=2/date=${dateStr}/"),
      dot_product_out_path = config.getString("dot_product_out_path", s"s3://thetradedesk-mlplatform-us-east-1/data/${ttdEnv}/audience/scores/tdid2seedid_raw/v=1/date=${dateStr}/"),
      density_split = config.getInt("density_split", -1),
      density_limit = config.getInt("density_limit", -1),
      tdid_limit = config.getInt("tdid_limit", -1),
      debug = config.getBoolean("debug", false),
      partition = config.getInt("partition", -1),
      sensitiveModel = config.getBoolean("sensitiveModel", true),
      minMaxSeedEmb = config.getDouble("minMaxSeedEmb", 1e-6),
      r = config.getDouble("r", 1e-8f).toFloat,
      loc_factor = config.getDouble("loc_factor", 0.8f).toFloat,
      samplingRate = config.getInt("sampling_rate", 3),
      // TdidSeedScoreScale
      raw_score_path = config.getString("raw_score_path", s"s3://thetradedesk-mlplatform-us-east-1/data/${ttdEnv}/audience/scores/tdid2seedid_raw/v=1/date=${dateStr}/"),
      full_out_path = config.getString("full_out_path", s"s3://thetradedesk-mlplatform-us-east-1/data/${ttdEnv}/audience/scores/tdid2seedid_all/v=1/date=${dateStr}/"),
      score_scale_out_path = config.getString("out_path", s"s3://thetradedesk-mlplatform-us-east-1/data/${ttdEnv}/audience/scores/tdid2seedid/v=1/date=${dateStr}/"),
      population_score_path = config.getString("population_score_path", s"s3://thetradedesk-mlplatform-us-east-1/data/${ttdEnv}/audience/scores/seedpopulationscore/v=1/date=${dateStr}/"),
      smooth_factor = config.getDouble("smooth_factor", 30f).toFloat,
      userLevelUpperCap = config.getDouble("userLevelUpperCap", 1e6f).toFloat,
      accuracy = config.getInt("accuracy", 1000),
      sampleRateForPercentile = config.getDouble("sampleRateForPercentile", 0.3),
      skipPercentile = config.getBoolean("skipPercentile", true),
      // TdidSeedScoreQualityCheck
      daysLookback = config.getInt("daysLookback", 1),
      percentileDiffThreshold = config.getDouble("percentileDiffThreshold", 0.002f),
      seedOutlinerPercentThreshold = config.getDouble("seedOutlinerPercentThreshold", 0.05f),
      runDate = date
    )
  }
}

