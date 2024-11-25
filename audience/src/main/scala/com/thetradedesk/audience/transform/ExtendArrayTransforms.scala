package com.thetradedesk.audience.transform

import org.apache.spark.sql.functions.udf

import java.util.concurrent.ThreadLocalRandom
import scala.reflect.runtime.universe.TypeTag

object ExtendArrayTransforms {
  val seedIdToSyntheticIdMapping =
    (mapping: Map[String, Int]) =>
      udf((origin: Array[String]) => {
        origin.flatMap(mapping.get)
      })

  val seedIdToSyntheticIdWithSamplingMapping =
    (mapping: Map[String, WeightPair]) =>
      udf((origin: Array[String]) => {
        origin
          .flatMap(mapping.get)
          .filter(e => e.weight >= 1 || ThreadLocalRandom.current().nextFloat() <= e.weight)
          .map(_.syntheticId)
      })
}

case class WeightPair(syntheticId: Int, weight: Float)
