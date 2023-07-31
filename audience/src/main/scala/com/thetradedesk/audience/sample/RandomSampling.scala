package com.thetradedesk.audience.sample

import com.thetradedesk.audience.datasets.AudienceModelPolicyRecord
import org.apache.spark.sql.functions._

import java.util.concurrent.ThreadLocalRandom

// used for oos to downsample the negative labels and keep all positive labels
object RandomSampling {
  val negativeSampleUDFGenerator = {
    (policyTable: Array[AudienceModelPolicyRecord]) =>
      udf((negSize: Int) => {
        
        val totalNegativeSyntheticIds = policyTable
          .map(e => (e.SyntheticId, ThreadLocalRandom.current().nextDouble()))
          .sortBy(_._2)
          .take(math.max(negSize, 1))
          .map(_._1)

        totalNegativeSyntheticIds
      })
  }
}


