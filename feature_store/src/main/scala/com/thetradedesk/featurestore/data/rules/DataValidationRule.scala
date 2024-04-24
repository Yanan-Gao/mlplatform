package com.thetradedesk.featurestore.data.rules

import upickle.default._

case class DataValidationRule private(value: Int)

object DataValidationRule {
  implicit val dataValidationRuleRW: ReadWriter[DataValidationRule] = readwriter[Int].bimap[DataValidationRule](_.value, intToDataType)

  val AtLeastOne = DataValidationRule(0)
  val AllExist = DataValidationRule(1)

  def intToDataType(value: Int) = {
    value match {
      case AtLeastOne.value => AtLeastOne
      case AllExist.value => AllExist
      case _ => AtLeastOne
    }
  }
}