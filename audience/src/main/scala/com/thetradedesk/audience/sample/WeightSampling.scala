package com.thetradedesk.audience.sample

import com.thetradedesk.audience.datasets.AudienceModelPolicyRecord
import org.apache.spark.sql.functions._

import java.util.concurrent.ThreadLocalRandom

object WeightSampling {

  val positiveSampleUDFGenerator =
    (policyTable: Map[Int, AudienceModelPolicyRecord], upperThreshold: Double, lowerThreshold: Double, smoothingFactor: Double, downSampleFactor: Double) =>
      udf((positiveSyntheticIds: Seq[Int]) => {

        val totalPositiveSyntheticIds = positiveSyntheticIds
          .map(e => (e, policyTable(e)))
          .filter(e => {
            if (e._2.ActiveSize*downSampleFactor < lowerThreshold) {
              false
            }
            else if (e._2.ActiveSize*downSampleFactor >= lowerThreshold && e._2.ActiveSize*downSampleFactor <= upperThreshold) {
              true
            }
            else {
              val randomValue = ThreadLocalRandom.current().nextDouble()
              randomValue < math.pow(upperThreshold / (e._2.ActiveSize*downSampleFactor), smoothingFactor)
            }
          })
          .map(e => e._1)
        totalPositiveSyntheticIds
      })

  val negativeSampleUDFGenerator = {
    (aboveThresholdPolicyTable: Array[AudienceModelPolicyRecord], upperThreshold: Double, labelDatasetSize: Long, downSampleFactor: Double) =>
      udf((negSize: Int) => {
        val negativeSyntheticIdsWithPolicy = aboveThresholdPolicyTable
          .map(e => (e, ThreadLocalRandom.current().nextDouble()))

        //        val adjustedWeights = negativeSyntheticIdsWithPolicy
        //          .map(e => if (e._1.Size > upper_threshold) (e._1.Size / all_seed_size, upper_threshold / all_seed_size) else (e._1.Size / all_seed_size, e._1.Size / all_seed_size))

        val totalNegativeSyntheticIds = negativeSyntheticIdsWithPolicy
          .map(
            e => (e._1.SyntheticId, -1 * math.log(e._2) / (math.min(e._1.ActiveSize*downSampleFactor, upperThreshold) / (labelDatasetSize - e._1.ActiveSize*downSampleFactor)))
          )
          .sortBy(_._2)
          .take(math.max(negSize, 1))
          .map(_._1)

        totalNegativeSyntheticIds
      })
  }

  val getLabels = (label: Float) => udf((ids: Seq[Int]) => Seq.fill(ids.length)(label))

  val zipAndGroupUDFGenerator =
    (maxLength: Int) =>
      udf((ids: Seq[Int], targets: Seq[Float]) => {
        ids.zip(targets).grouped(maxLength).toArray
      })

}


