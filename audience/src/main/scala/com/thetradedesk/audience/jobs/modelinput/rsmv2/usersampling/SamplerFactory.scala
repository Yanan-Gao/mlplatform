package com.thetradedesk.audience.jobs.modelinput.rsmv2.usersampling

object SamplerFactory {
  def fromString(name: String): Sampler = {
    name match {
      case "SIB" => SIBSampler
      case "RSMV2" => RSMV2Sampler
      case _ => throw new IllegalArgumentException(s"Unknown sampler: $name")
    }
  }
}
