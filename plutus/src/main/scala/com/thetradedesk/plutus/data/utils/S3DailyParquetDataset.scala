package com.thetradedesk.plutus.data.utils

import com.thetradedesk.plutus.data
import com.thetradedesk.spark.sql.SQLFunctions.DataFrameExtensions
import com.thetradedesk.spark.util.io.FSUtils.listDirectoryContents
import org.apache.spark.sql.{Dataset, Encoder, SaveMode, SparkSession}

import java.time.format.DateTimeFormatter
import java.time.{LocalDate, LocalDateTime}
import java.util.regex.Pattern
import scala.util.Try

abstract class S3DailyParquetDataset[T <: Product] {

  protected def genDateSuffix(date: LocalDate): String = {
    f"date=${date.getYear}${date.getMonthValue}%02d${date.getDayOfMonth}%02d"
  }

  /** Base S3 path, derived from the environment */
  protected def genBasePath(env: String): String

  /** Generates a full S3 path for a specific date */
  def genPathForDate(date: LocalDate, env: String): String = {
    s"${genBasePath(env)}/${genDateSuffix(date)}"
  }

  protected def genPathsForDateWithLookback(date: LocalDate, lookBack: Int, env: String): Seq[String] = {
    (0 to lookBack).map(i => genPathForDate(date.minusDays(i), env))
  }

  def writeData(date: LocalDate, dataset: Dataset[T], filecount: Int = 100, env: String = data.envForWrite): Unit = {
    dataset.coalesce(filecount)
      .write.mode(SaveMode.Overwrite)
      .parquet(genPathForDate(date, env))
  }

  def readDate(date: LocalDate, env: String, lookBack: Int = 0, nullIfColAbsent: Boolean = false)
                 (implicit encoder: Encoder[T], spark: SparkSession): Dataset[T] = {
    val paths = genPathsForDateWithLookback(date, lookBack, env)
    println(s"readDate called(date = $date, env=$env, lookback=$lookBack, nullIfColAbsent=$nullIfColAbsent)")
    println(s"readDate paths: ${paths.mkString(", ")}")
    spark.read.parquet(paths: _*)
      .selectAs[T](nullIfColAbsent)
  }

  protected def extractDateFromPath(path: String): Option[LocalDate] = {
    val datePattern = Pattern.compile("date=(\\d{8})")
    val matcher = datePattern.matcher(path)
    if (matcher.find()) {
      val dateString = matcher.group(1)
      Try(LocalDate.parse(dateString, DateTimeFormatter.ofPattern("yyyyMMdd"))).toOption
    } else {
      None
    }
  }

  def readLatestDataUpToIncluding(maxDate: LocalDate, env: String, lookBack: Int = 0, nullIfColAbsent: Boolean = false)
                                 (implicit encoder: Encoder[T], spark: SparkSession): Dataset[T] = {
    val basePath = genBasePath(env)
    val listEntries = listDirectoryContents(basePath)(spark)
    val allDates = listEntries
      .filter(_.isDirectory)
      .map(_.getPath.toString)
      .flatMap(extractDateFromPath)
      .distinct

    println(s"readLatestDataUpToIncluding maxDate: $maxDate")
    println(s"readLatestDataUpToIncluding lookBack: $lookBack")
    println(s"readLatestDataUpToIncluding basePath: $basePath")
    println(s"readLatestDataUpToIncluding allDates.length: ${allDates.length}")

    val paths = allDates.filter(date => date.isBefore(maxDate) || date.isEqual(maxDate))
      .sortWith(_.isAfter(_))
      .take(lookBack + 1)
      .map(genPathForDate(_, env))
      .toSeq

    println(s"readLatestDataUpToIncluding paths: ${paths.mkString(", ")}")

    if (paths.isEmpty) {
      throw new S3NoFilesFoundException(f"No paths found in ${basePath}")
    }

    spark.read.parquet(paths: _*)
      .selectAs[T](nullIfColAbsent)
  }
}

abstract class S3HourlyParquetDataset[T <: Product] extends S3DailyParquetDataset[T] {
  protected def genHourSuffix(datetime: LocalDateTime): String =
    f"hour=${datetime.getHour}"

  /** Generates a full S3 path for a specific datetime */
  def generatePathForHour(datetime: LocalDateTime, env: String): String = {
    s"${genPathForDate(datetime.toLocalDate, env)}/${genHourSuffix(datetime)}"
  }

  /* TODO: readDateHour */
}

class S3NoFilesFoundException(message: String) extends Exception(message)
