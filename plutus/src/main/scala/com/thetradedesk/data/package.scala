package com.thetradedesk

import com.thetradedesk.spark.sql.SQLFunctions.{ColumnExtensions, DataFrameExtensions}
import org.apache.spark.ml.linalg.{SparseVector, Vector}
import org.apache.spark.sql.expressions.UserDefinedFunction
import org.apache.spark.sql.functions.{col, lit, udf, when, xxhash64}
import org.apache.spark.sql.{Column, Dataset, Encoder, SparkSession}
import java.time.LocalDate

package object data {

  val PLUTUS_DATA_SOURCE = "plutus"
  val DEFAULT_SV_NAME = "google"


  def paddedDatePart(date: LocalDate, separator: Option[String]= Some("")): String = {
    separator match {
      case Some(s) => f"${date.getYear}$s${date.getMonthValue}%02d$s${date.getDayOfMonth}%02d"
    }
  }

  def explicitDatePart(date: LocalDate): String = {
    f"year=${date.getYear}/month=${date.getMonthValue}/day=${date.getDayOfMonth}"
  }

  def parquetDataPaths(s3path: String, date: LocalDate, source: Option[String] = None, lookBack: Option[Int] = None): Seq[String] = {
    source match {
      case Some(PLUTUS_DATA_SOURCE) => (0 to lookBack.getOrElse(0)).map(i => f"$s3path/${explicitDatePart(date)}")
      case _ => (0 to lookBack.getOrElse(0)).map(i => f"$s3path/date=${paddedDatePart(date)}")
    }
  }

  def cleansedDataPaths(basePath:String, date:LocalDate, lookBack: Option[Int] = None): Seq[String] = {
    (0 to lookBack.getOrElse(0)).map(i => f"${basePath}/${paddedDatePart(date.minusDays(i), separator = Some("/"))}/*/*/*.gz")
  }


  def plutusDataPath(s3Path: String, ttdEnv: String, prefix: String, svName: Option[String] = None, date: LocalDate): String = {
    s"$s3Path/$ttdEnv/$prefix/${svName.getOrElse(DEFAULT_SV_NAME)}/${explicitDatePart(date)}"
  }

  def plutusDataPaths(s3Path: String, ttdEnv: String, prefix: String, svName: Option[String] = None, date: LocalDate, lookBack: Option[Int] = None): Seq[String] = {
    (0 to lookBack.getOrElse(0)).map(i => f"${plutusDataPath(s3Path, ttdEnv, prefix, svName, date.minusDays(i))}")
  }

  def getParquetData[T: Encoder](s3path: String, date: LocalDate, source: Option[String] = None, lookBack: Option[Int] = None)(implicit spark: SparkSession): Dataset[T] = {
    val paths = parquetDataPaths(s3path, date, source, lookBack)
    spark.read.parquet(paths: _*)
      .selectAs[T]
  }

  // useful UDFs

  def getHashedCatCols(inputColAndDims: Seq[(String, Int)]): Array[Column] = {
    inputColAndDims.map(x =>
      when(col(x._1).isNotNullOrEmpty, shiftModUdf(xxhash64(col(x._1)) , lit(x._2) )).otherwise(0).alias(x._1)
    ).toArray
  }

  def getHashedIntCols(inputColAndDims: Seq[(String, Int)]): Array[Column] = {
    // collapse all negative values to zero (bad but we can fix later)
    inputColAndDims.map(x =>
      when(((col(x._1).isNotNull) && (col(x._1) >= 0)),  shiftModUdf(col(x._1), lit(x._2)) ).otherwise(0).alias(x._1)
    ).toArray
  }

  def nonNegativeMod = udf[Long, Int]( x => {
    val mod = Int.MaxValue -1
    val rawMod = x % mod
    rawMod + (if (rawMod < 0) mod else 0)
  })


  def shiftModUdf = udf((h: Long, m: Int) => {
    val i = h % m
    i + (if (i < 0) m + 1 else 1)
  })

  val vec_size: UserDefinedFunction = udf((v: Vector) => v.size)
  val vec_indices: UserDefinedFunction = udf((v: SparseVector) => v.indices)
  val vec_values: UserDefinedFunction = udf((v: SparseVector) => v.values)
}
