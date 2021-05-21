package com.thetradedesk.data.transform

import java.time.LocalDate
import com.thetradedesk.data._
import com.thetradedesk.data.schema.CleanInputData
import com.thetradedesk.spark.TTDSparkContext.spark.implicits._
import com.thetradedesk.spark.TTDSparkContext.spark
import org.apache.spark.sql.{Dataset, SparkSession}

object TrainingDataTransform {

  def inputDataPaths(s3Path: String, s3Prefix: String, ttdEnv: String, svName: Option[String], endDate: LocalDate, lookBack: Option[Int] = None): Seq[String] = {
    plutusDataPaths(s3Path, ttdEnv, s3Prefix, svName, endDate, lookBack)
  }

  def outputDataPaths(s3Path: String, s3Prefix: String, ttdEnv: String, svName: Option[String], endDate: LocalDate, lookBack: Option[Int] = None, dataType: String): String = {
    plutusDataPath(s3Path, ttdEnv, s3Prefix, svName, endDate) + "/" + f"lookback=${lookBack.get}/" + f"$dataType/"
  }

  def writeModelInputParquetData(ds: Dataset[CleanInputData] , s3Path: String, s3Prefix: String, ttdEnv: String, svName: Option[String], endDate: LocalDate, lookBack: Option[Int] = None, dataType: String): Unit = {
    ds.write.parquet(outputDataPaths(s3Path, s3Prefix, ttdEnv, svName, endDate, lookBack, dataType))
  }

  def loadInputData(s3Path: String, s3Prefix: String, ttdEnv: String, svName: Option[String], endDate: LocalDate, lookBack: Option[Int] = None)(implicit spark: SparkSession): Dataset[CleanInputData] = {
    import spark.implicits._
    spark.read.parquet(inputDataPaths(s3Path, s3Prefix, ttdEnv, svName, endDate, lookBack): _*).as[CleanInputData]
  }

  def createTemporalDataSplits(paths: Seq[String]) = {
    assert(paths.size >= 3, "cannot create temporal splits for less than 3 days of data")
    val p = paths.sorted.reverse

    val test = p.head
    val dev = p.tail.head
    val train = p.tail.tail
    (train, dev, test)
  }

  def createDataSplits(ds: Dataset[CleanInputData], splits: Option[Array[Double]] = Some(Array(0.8, 0.2)), seedValue: Option[Long] = Some(42L)): (Dataset[CleanInputData], Dataset[CleanInputData]) = {
    val s = ds.randomSplit(splits.get, seed = seedValue.get)
    (s(0), s(1))
  }

  def createComboSplits(paths: Seq[String]): (Dataset[CleanInputData], Dataset[CleanInputData], Dataset[CleanInputData]) = {
    val (trainPath, devPath, testPath) = createTemporalDataSplits(paths)
    val trainDevPaths: Seq[String] = trainPath :+ devPath
    val trainDev = spark.read.parquet(trainDevPaths: _*).as[CleanInputData]
    val test = spark.read.parquet(testPath).as[CleanInputData]

    val trainDevSplit = createDataSplits(trainDev)

    (trainDevSplit._1, trainDevSplit._2 , test)
  }
}
