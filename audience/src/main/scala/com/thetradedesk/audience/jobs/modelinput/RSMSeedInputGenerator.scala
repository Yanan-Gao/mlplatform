package com.thetradedesk.audience.jobs.modelinput

import com.thetradedesk.audience.datasets.CrossDeviceVendor.CrossDeviceVendor
import com.thetradedesk.audience.datasets._

/**
 * This class is used to generate model training samples for relevance score model
 * using the dataset provided by seed service
 */
case class RSMSeedInputGenerator(crossDeviceVendor: CrossDeviceVendor, override val sampleRate: Double) extends AudienceModelGraphInputGenerator("RSMSeed", crossDeviceVendor, sampleRate) {
  override def getAggregatedSeedReadableDataset(): LightReadableDataset[AggregatedSeedRecord] = AggregatedSeedReadableDataset()
}
