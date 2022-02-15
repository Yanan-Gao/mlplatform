package com.ttd.contextual.util.elDoradoUtilities.datasets.core

import java.time.LocalDate
import java.time.format.DateTimeFormatter

import com.ttd.contextual.util.elDoradoUtilities.io.FSUtils
import com.ttd.contextual.spark.TTDSparkContext
import com.ttd.contextual.spark.TTDSparkContext.spark
import com.ttd.contextual.util.elDoradoUtilities.io.FSUtils
import org.apache.spark.sql.DataFrame

trait S3FileReader {
  def readFiles(prefix: String,
                startDate: LocalDate,
                endDate: LocalDate,
                dateFormatterPattern: DateTimeFormatter,
                isInclusive: Boolean = true,
                hourSubPartitionPrefixIfPresent: Option[String] = Some("hour="),
                hourFormatterPattern: DateTimeFormatter = DateTimeFormatter.ofPattern(DefaultTimeFormatStrings.hourTimeFormatString),
                basePath: String = "",
                globPattern: String = ""): DataFrame = {

    val allPaths = if (hourSubPartitionPrefixIfPresent.isDefined) {
      val startDateTime = startDate.atStartOfDay()
      val endDateTime = endDate.atStartOfDay()

      val end = if (isInclusive) endDateTime.plusDays(1) else endDateTime
      val hourSubPartitionPrefix = hourSubPartitionPrefixIfPresent.get

      Iterator
        .iterate(startDateTime)(_.plusDays(1))
        .takeWhile(_.isBefore(end))
        .flatMap(curDate => {
          (0 to 23).map(currentHour => {
            val time = curDate.plusHours(currentHour)
            s"$prefix${curDate.format(dateFormatterPattern)}/$hourSubPartitionPrefix${time.format(hourFormatterPattern)}"
          })
        })
    } else {
      val end = if (isInclusive) endDate.plusDays(1) else endDate
      Iterator
        .iterate(startDate)(_.plusDays(1))
        .takeWhile(_.isBefore(end))
        .map(curDate => s"$prefix${curDate.format(dateFormatterPattern)}")
    }

    val paths = allPaths
      .filter{ path =>
        FSUtils.directoryExists(path) (TTDSparkContext.spark) }
      .map(path => s"$path/$globPattern")
      .toArray

    println(s"vertica files to include: ${paths.length} between ${startDate} and ${endDate}")
    println(paths.mkString(","))

    // base path is used to specify as an option for parquet reader.
    // with base path provided, date partition will be also converted to column.
    if (basePath.isEmpty)
      spark.read.parquet(paths: _*)
    else spark.read.option("basePath", basePath).parquet(paths: _*)
  }

}
