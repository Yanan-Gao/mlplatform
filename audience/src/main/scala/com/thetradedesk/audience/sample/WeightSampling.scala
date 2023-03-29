package com.thetradedesk.audience.sample

import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import scala.util.Random

object WeightSampling {

  val generatePositiveSample = udf((labels: Seq[Long], ids: Seq[Long], order_ids: Seq[Long], size: Seq[Double], upper_threshold:Double, lower_threshold:Double, smoothing_factor:Double, all_seed_size:Int) => {

    val TotalPositiveSamplesMap = ids.zip(size).zip(order_ids)
                                  .filter { case (id, _) => labels.contains(id) }
                                  .filter {case (id, size) => if (size< lower_threshold) {false} 
                                                                else if (size >= lower_threshold && size <= upper_threshold) {true} 
                                                                else {
                                                                          val randomValue = Random.nextDouble()
                                                                          randomValue < math.pow(upper_threshold/(size), smoothing_factor)
    }}
    
    val TotalPositiveSamples = TotalPositiveSamplesMap.map(_._1._1)
    val TotalPositiveOrderIds = TotalPositiveSamplesMap.map(_._2)
    
    (TotalPositiveSamples, TotalPositiveOrderIds)
  
})

  val generateNegativeSample = udf( (ids: Seq[Long], order_ids: Seq[Long], size: Seq[Double], adjustedSize:Seq[Double], upper_threshold:Double, lower_threshold:Double, all_seed_size:Int, neg_size:Int) => {
      val randomValues = ids.map(_ => Random.nextDouble())
      val adjustedWeights = size.map(value => if (value > upper_threshold) (value/all_seed_size, upper_threshold/all_seed_size) else (value/all_seed_size, value/all_seed_size))
      val keyValues = ids.zip(size).zip(adjustedSize).zip(randomValues).zip(order_ids)
                      .map{ case ((((id, size), adjustedSize), randomValue), order_ids) => (id, -1*math.log(randomValue)/(adjustedSize/(all_seed_size-size)), order_ids)}
    
      val TotalNegativeSamples = keyValues.sortBy(_._2).take(math.max(neg_size,1)).map(_._1)
      val TotalNegativeOrderIds = keyValues.sortBy(_._2).take(math.max(neg_size,1)).map(_._3)
      

      (TotalNegativeSamples, TotalNegativeOrderIds)

})

  val getLabels = udf((ids: Seq[Int], label:Int) => Seq.fill(ids.length)(label))

  val zipAndGroupUDF = udf((ids: Seq[Double], targets: Seq[Int], maxLength: Int) => {
      ids.zip(targets).grouped(maxLength).toArray
})

}


