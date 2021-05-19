package com.thetradedesk

import com.thetradedesk.spark.sql.SQLFunctions.DataFrameExtensions
import org.apache.spark.ml.linalg.{SparseVector, Vector}
import org.apache.spark.sql.expressions.UserDefinedFunction
import org.apache.spark.sql.functions.udf
import org.apache.spark.sql.{Dataset, Encoder, SparkSession}
import java.time.LocalDate

package object data {

  def paddedDatePart(date: LocalDate): String = {
    f"${date.getYear}/${date.getMonthValue}%02d/${date.getDayOfMonth}%02d"
  }

  def paddedDateNoSlashes(date: LocalDate): String = {
    f"${date.getYear}${date.getMonthValue}%02d${date.getDayOfMonth}%02d"
  }

  def datePart(date: LocalDate): String = {
    f"year=${date.getYear}/month=${date.getMonthValue}/day=${date.getDayOfMonth}"
  }

  def getParquetData[T: Encoder](s3path: String, date: LocalDate, source: Option[String] = None, lookBack: Option[Int] = None)(implicit spark: SparkSession): Dataset[T] = {
    val dateFormat = source match {
      case Some("plutus") => datePart(date)
      case _ => paddedDateNoSlashes(date)
    }
    val paths = (0 to lookBack.getOrElse(0)).map(i => f"$s3path/date=${dateFormat}")
    spark.read.parquet(paths: _*)
      .selectAs[T]
  }

  def getCleansedDataPath(basePath:String, date:LocalDate): String = {
    f"${basePath}/${paddedDatePart(date)}/*/*/*.gz"
  }

  def getCleansedDataLookbackPaths(basePath: String, endDate: LocalDate, lookBack: Int): Seq[String] = {
    (1 to lookBack).map(i => getCleansedDataPath(basePath, endDate.minusDays(i)))
  }

  // useful UDFs
  val vec_size: UserDefinedFunction = udf((v: Vector) => v.size)
  val vec_indices: UserDefinedFunction = udf((v: SparseVector) => v.indices)
  val vec_values: UserDefinedFunction = udf((v: SparseVector) => v.values)

}
