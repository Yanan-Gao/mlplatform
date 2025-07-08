package com.thetradedesk.audience.jobs.policytable

import com.thetradedesk.audience.datasets.Model
import com.thetradedesk.confetti.AutoConfigResolvingETLJobBase
import com.thetradedesk.spark.util.TTDConfig.config
import com.thetradedesk.spark.util.prometheus.PrometheusClient
import java.time.LocalDateTime

case class AudiencePolicyTableGeneratorJobConfig(
  modelName: String,
  lookBack: Int,
  date_time: String
)

object AudiencePolicyTableGeneratorJob
  extends AutoConfigResolvingETLJobBase[AudiencePolicyTableGeneratorJobConfig](
    env = config.getStringRequired("env"),
    experimentName = config.getStringOption("experimentName"),
    groupName = "audience",
    jobName = "AudiencePolicyTableGeneratorJob") {

  override val prometheus: Option[PrometheusClient] =
    Some(new PrometheusClient("AudienceModelJob", "AudiencePolicyTableGeneratorJob"))

  override def runETLPipeline(): Map[String, String] = {
    val conf = getConfig
    val dt = LocalDateTime.parse(conf.date_time)
    date = dt.toLocalDate
    dateTime = dt

    val model = Model.withName(conf.modelName)

    model match {
      case Model.RSM =>
        RSMGraphPolicyTableGenerator.generatePolicyTable()
      case Model.AEM =>
        AEMGraphPolicyTableGenerator.generatePolicyTable()
      case _ => throw new Exception(s"unsupported Model[$model]")
    }

    Map("status" -> "success")
  }
}