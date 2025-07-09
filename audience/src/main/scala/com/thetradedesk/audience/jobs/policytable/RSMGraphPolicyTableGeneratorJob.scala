package com.thetradedesk.audience.jobs.policytable

import com.thetradedesk.confetti.AutoConfigResolvingETLJobBase
import com.thetradedesk.spark.util.TTDConfig.config
import com.thetradedesk.spark.util.prometheus.PrometheusClient
import com.thetradedesk.audience.{date, dateTime}
import java.time.LocalDateTime

object RSMGraphPolicyTableGeneratorJob
  extends AutoConfigResolvingETLJobBase[AudiencePolicyTableGeneratorConfig](
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
    val generator = new RSMGraphPolicyTableGenerator(prometheus.get, conf)
    generator.generatePolicyTable()
    Map("status" -> "success")
  }
}
