package com.thetradedesk.featurestore.utils

import java.time.LocalDate
import java.time.format.{DateTimeFormatter, DateTimeParseException}
import scala.collection.mutable

object StringUtils {

  // replace the placeholders in string instance with overrides, e.g. date={dataValue} -> date=20250601
  def applyNamedFormat(instance: String, overrides: Map[String, String]): String = {
    overrides.foldLeft(instance) { case (str, (key, value)) =>
      str.replace(s"{$key}", value)
    }
  }

  def mergeAndApplyNamedFormat(overrides1: Map[String, String], overrides2: Map[String, String]): Map[String, String] = {
    val merged = overrides1 ++ overrides2
    val resolved = mutable.Map.empty[String, String]

    for ((key, value) <- merged) {
      val resolvedValue = applyNamedFormat(value, merged)
      resolved.put(key, resolvedValue)
    }
    resolved.toMap
  }

  def getDateFromString(dateStr: String, format: String = "yyyyMMdd"): LocalDate = {
    val formatter = DateTimeFormatter.ofPattern(format)
    try {
      LocalDate.parse(dateStr, formatter)
    } catch {
      case e: DateTimeParseException =>
        throw new IllegalArgumentException(s"Invalid date string: '$dateStr' or format: '$format'", e)
    }
  }

  def getDateStr(dt: LocalDate, format: String = "yyyyMMdd"): String = {
    val dtf = DateTimeFormatter.ofPattern(format)
    dt.format(dtf)
  }
}
