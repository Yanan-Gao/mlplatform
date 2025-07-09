package com.thetradedesk.audience.jobs.policytable

import com.thetradedesk.audience.{date, dateTime}
import com.thetradedesk.confetti.AutoConfigResolvingETLJobBase
import com.thetradedesk.spark.util.TTDConfig.config
import com.thetradedesk.spark.util.prometheus.PrometheusClient
import com.thetradedesk.audience.utils.S3Utils
import java.time.LocalDateTime

object RSMGraphPolicyTableGeneratorJob
  extends AutoConfigResolvingETLJobBase[AudiencePolicyTableGeneratorJobConfig](
    env = config.getStringRequired("env"),
    experimentName = config.getStringOption("experimentName"),
    groupName = "audience",
    jobName = "RSMGraphPolicyTableGeneratorJob") {

  override val prometheus: Option[PrometheusClient] =
    Some(new PrometheusClient("AudienceModelJob", "RSMGraphPolicyTableGeneratorJob"))

  override def runETLPipeline(): Map[String, String] = {
    val conf = getConfig
    val dt = LocalDateTime.parse(conf.date_time)
    date = dt.toLocalDate
    dateTime = dt
    val jobConf = conf.copy(
      seedMetadataS3Bucket = S3Utils.refinePath(conf.seedMetadataS3Bucket),
      seedMetadataS3Path = S3Utils.refinePath(conf.seedMetadataS3Path),
      seedRawDataS3Bucket = S3Utils.refinePath(conf.seedRawDataS3Bucket),
      seedRawDataS3Path = S3Utils.refinePath(conf.seedRawDataS3Path),
      policyS3Bucket = S3Utils.refinePath(conf.policyS3Bucket),
      policyS3Path = S3Utils.refinePath(conf.policyS3Path)
    )
    RSMGraphPolicyTableGenerator.generatePolicyTable(jobConf)
    Map("status" -> "success")
  }
}
