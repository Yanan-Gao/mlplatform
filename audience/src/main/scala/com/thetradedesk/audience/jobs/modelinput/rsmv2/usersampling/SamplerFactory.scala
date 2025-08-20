package com.thetradedesk.audience.jobs.modelinput.rsmv2.usersampling

/** Factory for building samplers based on name. The configuration type is kept
  * generic so that individual samplers can decide how to interpret the values
  * they need.
  */
object SamplerFactory {

  /**
    * Create a [[Sampler]] by name using an arbitrary configuration object.
    *
    * @param name identifier of the sampler to build
    * @param conf configuration object understood by the selected sampler
    * @return the concrete [[Sampler]] implementation
    */
  def fromString(name: String, conf: Any): Sampler = {
    name match {
      case "SIB" => SIBSampler
      case "RSMV2" => RSMV2Sampler(conf)
      case "RSMV2Measurement" =>
        RSMV2MeasurementSampler(conf) // used by measurement job to speed up process
      case "RSMV2Population" =>
        RSMV2PopulationSampler(conf) // used by measurement job to speed up process
      case _ => throw new IllegalArgumentException(s"Unknown sampler: $name")
    }
  }
}

