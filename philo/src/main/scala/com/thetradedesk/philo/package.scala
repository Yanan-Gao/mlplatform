package com.thetradedesk

import com.thetradedesk.geronimo.shared.{GERONIMO_DATA_SOURCE, explicitDatePart, paddedDatePart}
import com.thetradedesk.spark.util.io.FSUtils
import org.apache.spark.sql._
import org.apache.spark.sql.expressions.UserDefinedFunction
import org.apache.spark.sql.functions._

import java.time.LocalDate

package object philo { 

  /** shift hashed value to positive and reserve 0 for null values
   * copied from plutus
   *
   * @param hashValue value generated from xxhash64
   * @cardinality cardinality for the hash function
   * @return shifed value for hashing results
   */
  def shiftMod(hashValue: Long, cardinality: Int): Int = {
    val modulo = math.min(cardinality - 1, Int.MaxValue - 1)
    val index = (hashValue % modulo).intValue()
    // zero index is reserved for UNK and we do not want negative values
    val shift = if (index < 0) modulo + 1 else 1
    index + shift
  }
  /** convert it to a spark udf function
   * @return udf function for shiftMod
   */
  def shiftModUdf: UserDefinedFunction = udf((hashValue: Long, cardinality: Int) => {
    shiftMod(hashValue, cardinality)
  })

  // NOTE: this is TF record and you will need to add tf record package to the packages args when running spark submit for this to work
  def writeData(df: DataFrame, outputPath: String, ttdEnv: String, outputPrefix: String, date: LocalDate, partitions: Int, isTFRecord: Boolean = true): Unit = {

    // note the date part is year=yyyy/month=m/day=d/
    var func = df
      .repartition(partitions)
      .write
      .mode(SaveMode.Overwrite)

    if (isTFRecord) {
      func.format("tfrecord")
        .option("recordType", "Example")
        .option("codec", "org.apache.hadoop.io.compress.GzipCodec")
        .save(s"$outputPath/$ttdEnv/$outputPrefix/${explicitDatePart(date)}")
    } else {
      func.option("header", "true")
        .csv(s"$outputPath/$ttdEnv/$outputPrefix/${explicitDatePart(date)}")
    }
  }

  def tfRecordDataPaths(s3path: String, date: LocalDate, source: Option[String] = None, lookBack: Option[Int] = None, separator: Option[String] = None)(implicit spark: SparkSession): Seq[String] = {
    (source match {
      case Some(GERONIMO_DATA_SOURCE) => (0 to lookBack.getOrElse(0)).map(i => f"$s3path/${explicitDatePart(date.minusDays(i))}")
      case _ => (0 to lookBack.getOrElse(0)).map(i => f"$s3path/date=${paddedDatePart(date.minusDays(i), separator)}")
    }).filter(FSUtils.directoryExists(_))
  }

  // similar to loadParquetData, but for tfrecords. like all tfrecord parsing, you'll need the jar in spark-submit
  def loadTfRecordData[T: Encoder](s3path: String, date: LocalDate, source: Option[String] = None, lookBack: Option[Int] = None, dateSeparator: Option[String] = None)(implicit spark: SparkSession): DataFrame = {
    // even though it says parquet, there's nothing parquet specific in this method
    val paths = tfRecordDataPaths(s3path, date, source, lookBack, separator = dateSeparator)
    spark.read.format("tfrecord")
      .option("basePath", s3path)
      .load(paths: _*)
  }

  def flattenData(data: DataFrame, flatten_set: Set[String]): DataFrame = {
    data.select(
      data.columns.map(
        c => if (flatten_set.contains(c))
          col(s"${c}.value").alias(c).alias(c)
        else col(c)
      ): _*
    )
  }

  def addOriginalCols(keptCols: Seq[String], data: DataFrame): (DataFrame, Seq[String]) = {
    // add unhashed columns to output data
    val newData = keptCols.foldLeft(data) { (tempDF, colName) =>
      tempDF.withColumn(s"original$colName", col(colName))
    }
    val newColNames = keptCols.map(colName => s"original$colName")
    (newData, newColNames)
  }

  def countLinePerFile(outputPath: String, ttdEnv: String, outputPrefix: String, date: LocalDate)(implicit spark: SparkSession): DataFrame = {
    // even though it says parquet, there's nothing parquet specific in this method
    val df = spark.read.format("csv")
            .option("header", "true") // Adjust based on whether the files contain headers
            .csv(s"$outputPath/$ttdEnv/$outputPrefix/${explicitDatePart(date)}")
    val dfWithFileName = df.withColumn("full_file_name", input_file_name())
    val extractFileName = udf((fullPath: String) => {
      fullPath.split("/").last // Split by "/" and take the last part (the file name)
    })
    val dfWithBaseFileName = dfWithFileName.withColumn("file_name", extractFileName(col("full_file_name")))
    val lineCountsPerFile = dfWithBaseFileName
                           .groupBy("file_name")
                           .count()
    lineCountsPerFile
  }

  def debugInfo[T](varName: String, data: Dataset[T]): Unit = {
    println(s"$varName")
    println("---------------------------------------")
    data.printSchema()
    println("---------------------------------------")
  }
}