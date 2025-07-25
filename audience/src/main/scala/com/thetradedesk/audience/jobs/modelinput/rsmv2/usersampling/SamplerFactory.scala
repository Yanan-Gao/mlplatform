package com.thetradedesk.audience.jobs.modelinput.rsmv2.usersampling

import com.thetradedesk.audience.jobs.modelinput.rsmv2.RelevanceModelInputGeneratorJobConfig

object SamplerFactory {
  def fromString(name: String, conf: RelevanceModelInputGeneratorJobConfig): Sampler = {
    name match {
      case "SIB" => SIBSampler
      case "RSMV2" => RSMV2Sampler(conf)
      case "RSMV2Measurement" => RSMV2MeasurementSampler(conf) // will help measurement job use split=1 in feature_store to speed up the process
      case "RSMV2Population" => RSMV2PopulationSampler(conf) // will help measurement job use split=1 in feature_store to speed up the process
      case _ => throw new IllegalArgumentException(s"Unknown sampler: $name")
    }
  }
}
