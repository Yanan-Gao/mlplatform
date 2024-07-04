package com.thetradedesk.audience.jobs.policytable

import com.thetradedesk.audience.datasets.Model
import com.thetradedesk.spark.util.TTDConfig.config
import com.thetradedesk.spark.util.prometheus.PrometheusClient

object AudiencePolicyTableGeneratorJob {
  val prometheus = new PrometheusClient("AudienceModelJob", "AudiencePolicyTableGeneratorJob")

  object Config {
    val model = Model.withName(config.getString("modelName", default = "RSM"))
    val lookBack = config.getInt("lookBack", default = 3)
  }

  def main(args: Array[String]): Unit = {
    runETLPipeline()
    prometheus.pushMetrics()
  }

  def runETLPipeline(): Unit = {
    Config.model match {
      case Model.RSM =>
        RSMGraphPolicyTableGenerator.generatePolicyTable()
      case Model.AEM =>
        AEMGraphPolicyTableGenerator.generatePolicyTable()
      case _ => throw new Exception(s"unsupported Model[${Config.model}]")
    }
  }
}