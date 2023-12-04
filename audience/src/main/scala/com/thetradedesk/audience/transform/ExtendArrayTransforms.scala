package com.thetradedesk.audience.transform

import org.apache.spark.sql.functions.udf
import scala.reflect.runtime.universe.TypeTag

object ExtendArrayTransforms {
  val seedIdToSyntheticIdMapping =
    (mapping: Map[String, Int]) =>
      udf((origin: Array[String]) => {
        origin.map(mapping.getOrElse(_, -1)).filter(_ >= 0)
      })
}
