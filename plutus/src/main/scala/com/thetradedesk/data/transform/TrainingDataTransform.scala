package com.thetradedesk.data.transform

import java.time.LocalDate

import com.thetradedesk.data.load.TfRecordWriter._
import com.thetradedesk.data._
import com.thetradedesk.data.schema.CleanInputData
import com.thetradedesk.spark.TTDSparkContext.spark.implicits._
import com.thetradedesk.spark.TTDSparkContext.spark
import com.thetradedesk.spark.sql.SQLFunctions._
import org.apache.spark.sql.{Column, Dataset, SparkSession}

object TrainingDataTransform {

  def inputDataPaths(s3Path: String, s3Prefix: String, ttdEnv: String, svName: Option[String], endDate: LocalDate, lookBack: Option[Int] = None): Seq[String] = {
    plutusDataPaths(s3Path, ttdEnv, s3Prefix, svName, endDate, lookBack)
  }

  def outputDataPaths(s3Path: String, s3Prefix: String, ttdEnv: String, svName: Option[String], endDate: LocalDate, lookBack: Option[Int] = None, dataType: String, fileType: String): String = {
    plutusDataPath(s3Path, ttdEnv, s3Prefix, svName, endDate) + "/" + f"lookback=${lookBack.get}/" + f"$fileType/" + f"$dataType/"
  }

  def writeModelInputData(ds: Dataset[CleanInputData], s3Path: String, s3Prefix: String, ttdEnv: String, svName: Option[String], endDate: LocalDate, lookBack: Option[Int] = None, dataType: String, fileType: String, selection: Array[Column] ): Unit = {
    val output = outputDataPaths(s3Path, s3Prefix, ttdEnv, svName, endDate, lookBack, dataType, fileType)
    fileType match {
      case "parquet" => ds.select(selection: _*).write.parquet(output)
      case "tfrecords" | "tfrecord" => writeData(ds.toDF(), selection, output)
      case _ => sys.error("unrecognized file type in writeModelInputData. please investigate")
    }
  }

  def loadInputData(s3Path: String, s3Prefix: String, ttdEnv: String, svName: Option[String], endDate: LocalDate, lookBack: Option[Int] = None)(implicit spark: SparkSession): Dataset[CleanInputData] = {
    import spark.implicits._
    spark.read.parquet(inputDataPaths(s3Path, s3Prefix, ttdEnv, svName, endDate, lookBack): _*).selectAs[CleanInputData]
  }

  def createTemporalDataSplits(paths: Seq[String]) = {
    assert(paths.size >= 3, "cannot create temporal splits for less than 3 days of data")
    val p = paths.sorted.reverse

    val testAndVal = p.head
    val train = p.tail
    (train, testAndVal)
  }

  def createDataSplits(ds: Dataset[CleanInputData], splits: Option[Array[Double]] = Some(Array(0.5, 0.5)), seedValue: Option[Long] = Some(42L)): (Dataset[CleanInputData], Dataset[CleanInputData]) = {
    val s = ds.randomSplit(splits.get, seed = seedValue.get)
    (s(0), s(1))
  }

  def createComboSplits(paths: Seq[String]): (Dataset[CleanInputData], Dataset[CleanInputData], Dataset[CleanInputData]) = {
    val (trainPath, testAndValPath) = createTemporalDataSplits(paths)
    val train = spark.read.parquet(trainPath: _*).selectAs[CleanInputData]
    val testAndVal = spark.read.parquet(testAndValPath).selectAs[CleanInputData]

    val (validation, test) = createDataSplits(testAndVal)

    (train, validation , test)
  }
}
