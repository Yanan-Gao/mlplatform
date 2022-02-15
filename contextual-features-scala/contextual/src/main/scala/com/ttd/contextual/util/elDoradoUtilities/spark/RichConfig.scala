package com.ttd.contextual.util.elDoradoUtilities.spark

import java.time.format.DateTimeFormatter
import java.time.{Duration, LocalDate, LocalDateTime, ZoneOffset}

import com.typesafe.config.Config

object RichConfig {
  //noinspection LanguageFeature
  implicit def configToRichConfig(config: Config): RichConfig = new RichConfig(config)

  def currentDateUTC(): LocalDate = LocalDate.now(ZoneOffset.UTC)
}

case class RichConfig(underlying: Config) {

  private def getOption[T](path: String, getFunc: (String) => T): Option[T] = {
    if (underlying.hasPath(path)) {
      System.err.println(s"Reading config for $path, found value ${underlying.getString(path)}")
      Some(getFunc(path))
    } else {
      System.err.println(s"Reading config for $path, found no value")
      None
    }
  }

  private def getRequired[T](path: String, getFunc: (String) => T): T = {
    val opt = getOption(path, getFunc)
    if (opt.isEmpty) {
      System.err.println(s"Config for $path is required, throwing exception")
      throw new IllegalArgumentException(s"Config for $path is required")
    }
    opt.get
  }

  private def getWithDefault[T](path: String, getFunc: (String) => T, default: T): T = {
    val res = getOption(path, getFunc)
    if (res.isEmpty) {
      System.err.println(s"Using default of $default for $path")
      default
    } else res.get
  }

  def getBoolean(path: String, default: Boolean): Boolean = getWithDefault(path, underlying.getBoolean, default)
  def getBooleanOption(path: String): Option[Boolean] = getOption(path, underlying.getBoolean)
  def getBooleanRequired(path: String): Boolean = getRequired(path, underlying.getBoolean)

  def getInt(path: String, default: Int): Int = getWithDefault(path, underlying.getInt, default)
  def getIntOption(path: String): Option[Int] = getOption(path, underlying.getInt)
  def getIntRequired(path: String): Int = getRequired(path, underlying.getInt)

  def getLong(path: String, default: Long): Long = getWithDefault(path, underlying.getLong, default)
  def getLongOption(path: String): Option[Long] = getOption(path, underlying.getLong)
  def getLongRequired(path: String): Long = getRequired(path, underlying.getLong)

  def getDouble(path: String, default: Double): Double = getWithDefault(path, underlying.getDouble, default)
  def getDoubleOption(path: String): Option[Double] = getOption(path, underlying.getDouble)
  def getDoubleRequired(path: String): Double = getRequired(path, underlying.getDouble)

  def getString(path: String, default: String): String = getWithDefault(path, underlying.getString, default)
  def getStringOption(path: String): Option[String] = getOption(path, underlying.getString)
  def getStringRequired(path: String): String = getRequired(path, underlying.getString)

  def getDuration(path: String, default: Duration): Duration = getWithDefault(path, underlying.getDuration, default)
  def getDurationOption(path: String): Option[Duration] = getOption(path, underlying.getDuration)
  def getDurationRequired(path: String): Duration = getRequired(path, underlying.getDuration)

  // These are based on getString*
  def getParsedOption[T](path: String, parseFunc: (String) => T): Option[T] = {
    getStringOption(path).map(parseFunc)
  }
  def getParsedRequired[T](path: String, parseFunc: (String) => T): T = {
    parseFunc(getStringRequired(path))
  }
  def getParsedWithDefault[T](path: String, parseFunc: (String) => T, default: T): T = {
    getParsedOption(path, parseFunc).getOrElse(default)
  }

  // Date
  private def dateParser(p: String): LocalDate = LocalDate.parse(p)
  def getDate(path: String, default: LocalDate): LocalDate = getParsedWithDefault(path, dateParser, default)
  def getDateOption(path: String): Option[LocalDate] = getParsedOption(path, dateParser)
  def getDateRequired(path: String): LocalDate = getParsedRequired(path, dateParser)

  // DateWithFormat
  def getDateWithFormat(path: String, default: LocalDate, dateFormat: String): LocalDate =
    getParsedWithDefault(path, LocalDate.parse(_, DateTimeFormatter.ofPattern(dateFormat)), default)
  def getDateWithFormatOption(path: String, dateFormat: String): Option[LocalDate] =
    getParsedOption(path, LocalDate.parse(_, DateTimeFormatter.ofPattern(dateFormat)))
  def getDateWithFormatRequired(path: String, dateFormat: String): LocalDate =
    getParsedRequired(path, LocalDate.parse(_, DateTimeFormatter.ofPattern(dateFormat)))

  // DateTime
  private def dateTimeParser(p: String): LocalDateTime = LocalDateTime.parse(p)
  def getDateTime(path: String, default: LocalDateTime): LocalDateTime =
    getParsedWithDefault(path, dateTimeParser, default)
  def getDateTimeOption(path: String): Option[LocalDateTime] = getParsedOption(path, dateTimeParser)
  def getDateTimeRequired(path: String): LocalDateTime = getParsedRequired(path, dateTimeParser)

  // StringSeq
  private def stringSeqParser(value: String): Seq[String] = value.split(",").map(_.trim).toSeq
  def getStringSeq(path: String, default: Seq[String]): Seq[String] = getParsedWithDefault(path, stringSeqParser, default)
  def getStringSeqOption(path: String): Option[Seq[String]] = getParsedOption(path, stringSeqParser)
  def getStringSeqRequired(path: String): Seq[String] = getParsedRequired(path, stringSeqParser)
}

