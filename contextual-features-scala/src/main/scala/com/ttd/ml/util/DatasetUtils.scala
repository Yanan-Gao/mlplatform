package com.ttd.ml.util

import java.time.LocalDate

import com.ttd.ml.util.elDoradoUtilities.datasets.core.DatePartitionedS3DataSet
import com.ttd.ml.spark.TTDSparkContext.spark
import org.apache.spark.sql.{Dataset, Encoder, Encoders}
import org.joda.time.{DateTime, Days}
import org.joda.time.format.{DateTimeFormat, DateTimeFormatter}

import scala.reflect.runtime.universe.TypeTag

/** Util Functions that help list a range of dates before reading from spark */
object DatasetUtils {
  def formatDate(date: String): String = {
    val originalFormat: DateTimeFormatter = DateTimeFormat.forPattern("yyyyMMdd")
    val newFormat: DateTimeFormatter = DateTimeFormat.forPattern("yyyy-MM-dd")
    val input: DateTime = originalFormat.parseDateTime(date)
    newFormat.print(input)
  }

  def getDateRange(startDate: String, endDate: String): String = {
    val start = new DateTime(formatDate(startDate))
    val end = new DateTime(formatDate(endDate))
    val numDays = Days.daysBetween(start, end).getDays
    val range = (for (f <- 0 to numDays) yield {
      start.plusDays(f).toLocalDate.toString("yyyyMMdd").toInt
    }).mkString(", ")
    "{" + range + "}"
  }

  def previousExists[T <: Product : TypeTag](d: DatePartitionedS3DataSet[T], startDate: LocalDate, endDate: LocalDate): Dataset[T] = {
    // Filter for the partition in question and check if it exists
    try {
      val df = d.readRange(startDate, endDate, isInclusive = true)
      assert(df.head(1).nonEmpty, "Previous partition does not exist")
      df
    } catch {
      // If no partitions exist at all, it will error attempting to filter
      case _: Exception => {
        implicit val encoder: Encoder[T] = Encoders.product[T]
        spark.emptyDataset[T]
      }
    }
  }
}
