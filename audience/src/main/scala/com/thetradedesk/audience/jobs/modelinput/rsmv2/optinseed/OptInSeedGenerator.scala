package com.thetradedesk.audience.jobs.modelinput.rsmv2.optinseed

import com.thetradedesk.audience.jobs.modelinput.rsmv2.datainterface.OptInSeedRecord
import org.apache.spark.sql.Dataset

trait OptInSeedGenerator {

  def generate() : Dataset[OptInSeedRecord]

}
