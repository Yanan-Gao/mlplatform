package com.thetradedesk.featurestore.transform

import org.apache.spark.sql.functions.udf

object DensityScoreFilterUDF {

  // lowerThreshold: inclusive, upperThreshold: exclusive
  def apply(lowerThreshold: Float, upperThreshold: Float) = udf(
    (pairs : Seq[(Int, Float)]) =>
      pairs.filter(e => e._2 < upperThreshold && e._2 >= lowerThreshold).map(_._1)
  )
}