package com.thetradedesk.featurestore.utils

object StringUtils {

  // replace the placeholders in string instance with overrides, e.g. date={dataValue} -> date=20250601
  def applyNamedFormat(instance: String, overrides: Map[String, String]): String = {
    overrides.foldLeft(instance) { case (str, (key, value)) =>
      str.replace(s"{$key}", value)
    }
  }
}
