package com.thetradedesk.audience.jobs.modelinput

import com.thetradedesk.audience.datasets.CrossDeviceVendor.CrossDeviceVendor
import com.thetradedesk.audience.datasets._

case class AEMConversionGraphInputGenerator(crossDeviceVendor: CrossDeviceVendor, override val sampleRate: Double) extends AudienceModelGraphInputGenerator("AEMConversionGraph", crossDeviceVendor, sampleRate) {
  override def getAggregatedSeedReadableDataset(): LightReadableDataset[AggregatedSeedRecord] = AggregatedConversionPixelReadableDataset()
}
