package com.thetradedesk.data.transform

import java.time.LocalDate
import com.thetradedesk.data._
import com.thetradedesk.data.schema.CleanInputData
import com.thetradedesk.spark.TTDSparkContext.spark.implicits._
import com.thetradedesk.spark.TTDSparkContext.spark
import org.apache.spark.sql.{Dataset, SparkSession}

object TrainingDataTransform {

  def inputDataPaths(s3Path: String, s3Prefix: String,  ttdEnv: String, svName: Option[String], endDate: LocalDate, lookBack: Option[Int] = None): Seq[String] = {
    plutusDataPaths(s3Path, ttdEnv, s3Prefix, svName, endDate, lookBack)
  }

  def loadInputData(s3Path: String, s3Prefix: String,  ttdEnv: String, svName: Option[String], endDate: LocalDate, lookBack: Option[Int] = None)(implicit spark: SparkSession): Dataset[CleanInputData] ={
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

  def createDataSplits(ds: Dataset[CleanInputData], splits: Option[Array[Double]] = Some(Array(0.8, 0.1, 0.1)), seedValue: Option[Long] = Some(42L)): (Dataset[CleanInputData], Dataset[CleanInputData], Dataset[CleanInputData]) = {
    val s = ds.randomSplit(splits.get, seed = seedValue.get)
    (s(0), s(1), s(2))
  }
}
