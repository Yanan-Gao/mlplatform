package com.thetradedesk.audience.jobs.modelinput.rsmv2.usersampling

object SamplerFactory {
  def fromString(name: String): Sampler = {
    name match {
      case "SIB" => SIBSampler
      case "RSMV2" => RSMV2Sampler
      case "RSMV2Measurement" => RSMV2MeasurementSampler // will help measurement job use split=1 in feature_store to speed up the process
      case "RSMV2Population" => RSMV2PopulationSampler // will help measurement job use split=1 in feature_store to speed up the process
      case _ => throw new IllegalArgumentException(s"Unknown sampler: $name")
    }
  }
}
