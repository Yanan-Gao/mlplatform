package com.ttd.contextual.util.elDoradoUtilities.datasets.core

import java.time.LocalDate
import java.time.format.DateTimeFormatter

import org.apache.spark.sql.Dataset

trait DailyLogsReader[T <: Product] {
  def readDates(dates: Seq[LocalDate]): Dataset[T]
}

class DailyLogsReaderS3DataSet[T <: Product](rootFolderPath: String, fileFormat: FileFormat)(implicit m: Manifest[T])
  extends S3DataSet[T](SourceDataSet, S3Roots.LOGS2_ROOT, rootFolderPath, fileFormat)
  with DailyLogsReader[T] {

  override protected val s3RootProd: String = normalizeUri(s"$s3Root")
  override protected val s3RootTest: String = normalizeUri(s"$s3Root")

  def readDates(dates: Seq[LocalDate]): Dataset[T] = {
    super.read(dates.map(date => s"${normalizeUri(rootFolderPath)}${date.format(DateTimeFormatter.ofPattern("yyyy/MM/dd"))}/*/*"): _*)
  }
}
