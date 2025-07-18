package com.thetradedesk.audience.jobs.policytable

import com.thetradedesk.confetti.AutoConfigResolvingETLJobBase
import com.thetradedesk.spark.util.TTDConfig.config
import com.thetradedesk.spark.util.prometheus.PrometheusClient
import com.thetradedesk.audience.{date, dateTime}
import java.time.LocalDateTime

object RSMGraphPolicyTableGeneratorJob
  extends AutoConfigResolvingETLJobBase[AudiencePolicyTableGeneratorConfig](
    groupName = "audience",
    jobName = "RSMGraphPolicyTableGeneratorJob") {

  override val prometheus: Option[PrometheusClient] =
    Some(new PrometheusClient("AudienceModelJob", "RSMGraphPolicyTableGeneratorJob"))

  override def runETLPipeline(): Unit = {
    val conf = getConfig
    val generator = new RSMGraphPolicyTableGenerator(prometheus.get, conf)
    generator.generatePolicyTable()
  }
}
