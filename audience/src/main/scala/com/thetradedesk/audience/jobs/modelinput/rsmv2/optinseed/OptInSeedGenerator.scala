package com.thetradedesk.audience.jobs.modelinput.rsmv2.optinseed

import com.thetradedesk.audience.jobs.modelinput.rsmv2.datainterface.OptInSeedRecord
import com.thetradedesk.audience.jobs.modelinput.rsmv2.RelevanceModelInputGeneratorJobConfig
import org.apache.spark.sql.Dataset

trait OptInSeedGenerator {

  def generate(conf: RelevanceModelInputGeneratorJobConfig) : Dataset[OptInSeedRecord]

}
