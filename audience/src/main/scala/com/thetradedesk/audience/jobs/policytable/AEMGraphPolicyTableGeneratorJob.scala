package com.thetradedesk.audience.jobs.policytable

import com.thetradedesk.confetti.AutoConfigResolvingETLJobBase
import com.thetradedesk.spark.util.TTDConfig.config
import com.thetradedesk.spark.util.prometheus.PrometheusClient
import com.thetradedesk.audience.{date, dateTime}
import java.time.LocalDateTime

object AEMGraphPolicyTableGeneratorJob
  extends AutoConfigResolvingETLJobBase[AudiencePolicyTableGeneratorConfig](
    groupName = "audience",
    jobName = "AEMGraphPolicyTableGeneratorJob") {

  override val prometheus: Option[PrometheusClient] =
    Some(new PrometheusClient("AudienceModelJob", "AEMGraphPolicyTableGeneratorJob"))

  override def runETLPipeline(): Map[String, String] = {
    val conf = getConfig
    val dt = LocalDateTime.parse(conf.date_time)
    date = dt.toLocalDate
    dateTime = dt
    val generator = new AEMGraphPolicyTableGenerator(prometheus.get, conf)
    generator.generatePolicyTable()
    Map("status" -> "success")
  }
}
