package com.thetradedesk.confetti.utils

import scala.reflect.ClassTag
import scala.reflect.runtime.universe._

object ConfigFactory {
  def fromMap[T: TypeTag: ClassTag](map: Map[String, String], postProcess: T => T = (t: T) => t): T = {
    val raw = MapConfigReader.read[T](map)
    postProcess(raw)
  }
}
