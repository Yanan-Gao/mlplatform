package com.thetradedesk.audience.jobs.policytable

import com.thetradedesk.audience.datasets.StorageCloud
import com.thetradedesk.audience._
import com.thetradedesk.audience.utils.S3Utils
import com.thetradedesk.confetti.AutoConfigResolvingETLJobBase
import com.thetradedesk.spark.util.TTDConfig.config
import com.thetradedesk.spark.util.prometheus.PrometheusClient

object RSMGraphPolicyTableGeneratorJob
  extends AutoConfigResolvingETLJobBase[AudiencePolicyTableGeneratorConfig](
    groupName = "audience",
    jobName = "RSMGraphPolicyTableGeneratorJob") {

  override val prometheus: Option[PrometheusClient] =
    Some(new PrometheusClient("AudienceModelJob", "RSMGraphPolicyTableGeneratorJob"))

  override def runETLPipeline(): Unit = {
    val conf = getConfig
    val generator = new RSMGraphPolicyTableGenerator(prometheus.get, confettiEnv, experimentName, conf)
    generator.generatePolicyTable()
  }

  /**
   * for backward compatibility, local test usage.
   * */
  override def loadLegacyConfig(): AudiencePolicyTableGeneratorConfig = {

    AudiencePolicyTableGeneratorConfig(
      // config to determine which cloud storage source to use
      storageCloud = config.getString("storageCloud", StorageCloud.AWS.toString),
      // detect recent seed metadata path in airflow and pass to spark job
      seedMetaDataRecentVersion = Option(config.getString("seedMetaDataRecentVersion", null)),
      seedMetadataS3Bucket = S3Utils.refinePath(config.getString("seedMetadataS3Bucket", "ttd-datprd-us-east-1")),
      countryDensityThreshold = config.getDouble("countryDensityThreshold", 0.8),
      seedMetadataS3Path = S3Utils.refinePath(config.getString("seedMetadataS3Path", "prod/data/SeedDetail/v=1/")),
      seedRawDataS3Bucket = S3Utils.refinePath(config.getString("seedRawDataS3Bucket", "ttd-datprd-us-east-1")),
      seedRawDataS3Path = S3Utils.refinePath(config.getString("seedRawDataS3Path", "prod/data/Seed/v=1")),
      seedRawDataRecentVersion = Option(config.getString("seedRawDataRecentVersion", null)),
      policyTableResetSyntheticId = config.getBoolean("policyTableResetSyntheticId", false),
      // conversion data look back days
      conversionLookBack = config.getInt("conversionLookBack", 5),
      expiredDays = config.getInt("expiredDays", default = 7),
      policyTableLookBack = config.getInt("policyTableLookBack", default = 3),
      policyS3Bucket = S3Utils.refinePath(config.getString("policyS3Bucket", "thetradedesk-mlplatform-us-east-1")),
      policyS3Path = S3Utils.refinePath(config.getString("policyS3Path", s"configdata/${ttdEnv}/audience/policyTable/RSM/v=1")),
      maxVersionsToKeep = config.getInt("maxVersionsToKeep", 30),
      reuseAggregatedSeedIfPossible = config.getBoolean("reuseAggregatedSeedIfPossible", true),
      bidImpressionRepartitionNum = config.getInt("bidImpressionRepartitionNum", 4096),
      seedRepartitionNum = config.getInt("seedRepartitionNum", 32),
      bidImpressionLookBack = config.getInt("bidImpressionLookBack", 1),
      graphUniqueCountKeepThreshold = config.getInt("graphUniqueCountKeepThreshold", 20),
      graphScoreThreshold = config.getDouble("graphScoreThreshold", 0.01),
      seedJobParallel = config.getInt("seedJobParallel", Runtime.getRuntime.availableProcessors()),
      seedProcessLowerThreshold = config.getLong("seedProcessLowerThreshold", 2000),
      seedProcessUpperThreshold = config.getLong("seedProcessUpperThreshold", 100000000),
      ttdOwnDataUpperThreshold = config.getLong("ttdOwnDataUpperThreshold", 200000000),
      seedExtendGraphUpperThreshold = config.getLong("seedExtendGraphUpperThreshold", 3000000),
      activeUserRatio = config.getDouble("activeUserRatio", 0.4),
      aemPixelLimit = config.getInt("aemPixelLimit", 5000),
      selectedPixelsConfigPath = config.getString("selectedPixelsConfigPath", "s3a://thetradedesk-mlplatform-us-east-1/configdata/prodTest/audience/other/AEM/selectedPixelTrackingTagIds/"),
      useSelectedPixel = config.getBoolean("useSelectedPixel", false),
      campaignFlightStartingBufferInDays = config.getInt("campaignFlightStartingBufferInDays", 14),
      allRSMSeed = config.getBoolean("allRSMSeed", false),
      activeAdvertiserLookBackDays = config.getInt("activeAdvertiserLookBackDays", 180),
      newSeedLookBackDays = config.getInt("newSeedLookBackDays", 7),

      audioGraphExtensionRatio = config.getDouble("audioGraphExtensionRatio", 0.3),
      ctvGraphExtensionRatio = config.getDouble("ctvGraphExtensionRatio", 0.3),
      userDownSampleHitPopulation = config.getInt("userDownSampleHitPopulation", 1000000),
      saltToSampleUser = config.getString("saltToSampleUser", "0BgGCE"),
      densityFeatureScoreNewSeedPrefix = config.getString("densityFeatureScoreNewSeedPrefix", "s3a://thetradedesk-mlplatform-us-east-1/features/feature_store/prod/profiles/source=bidsimpression/index=SeedId/metadata/newSeed/v=1/"),
      readSeedDetailMode = config.getBoolean("readSeedDetailMode", true),
      runDate = date,

    )


  }
}
