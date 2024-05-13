package com.thetradedesk.featurestore.configs

import upickle.default._

import java.time.LocalDateTime
import java.time.format.DateTimeFormatter

case class DataSource private(value: Int)

object DataSource {
  implicit val dataSourceRW: ReadWriter[DataSource] = readwriter[Int].bimap[DataSource](_.value, intToDataType)

  val Logs = DataSource(0)
  val Geronimo = DataSource(1)
  val DailyJob = DataSource(2)
  val HourlyJob = DataSource(3)

  def intToDataType(value: Int) = {
    value match {
      case Logs.value => Logs
      case Geronimo.value => Geronimo
      case HourlyJob.value => HourlyJob
      case _ => DailyJob
    }
  }

  lazy val dateFormatter = DateTimeFormatter.ofPattern("yyyyMMdd")
  lazy val logsDateFormatter = DateTimeFormatter.ofPattern("yyyy/MM/dd")
  lazy val crossDeviceDateFormatter = DateTimeFormatter.ofPattern("yyyy-MM-dd")

  def basePath(dataSource: DataSource, dateTime: LocalDateTime): String = dataSource match {
    case DataSource.Logs => s"${dateTime.format(logsDateFormatter)}"
    case DataSource.Geronimo => s"year=${dateTime.getYear}/month=${dateTime.getMonthValue}%02d/day=${dateTime.getDayOfMonth}%02d"
    case DataSource.HourlyJob => s"date=${dateTime.format(dateFormatter)}/hour=${dateTime.getHour}"
    case _ => s"date=${dateTime.format(dateFormatter)}"
  }
}
