package com.thetradedesk.audience.jobs.modelinput.rsmv2.optinseed

object OptInSeedFactory {
  def fromString(name: String, filterExpr: String): OptInSeedGenerator = {
    name match {
      case "Full" => FullSeedGenerator
      case "Active" => ActiveSeedGenerator
      case "Dynamic" => new DynamicSeedGenerator(filterExpr)
      case _ => throw new IllegalArgumentException(s"Unknown sampler: $name")
    }
  }

}
