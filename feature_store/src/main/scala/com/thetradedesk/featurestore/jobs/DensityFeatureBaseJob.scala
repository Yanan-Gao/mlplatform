package com.thetradedesk.featurestore.jobs

import com.thetradedesk.featurestore._
import com.thetradedesk.featurestore.rsm.CommonEnums.{CrossDeviceVendor, DataSource}
import com.thetradedesk.geronimo.bidsimpression.schema.BidsImpressions
import com.thetradedesk.spark.TTDSparkContext.spark
import com.thetradedesk.spark.TTDSparkContext.spark.implicits._
import com.thetradedesk.spark.util.io.FSUtils
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{DataFrame, SaveMode}

import java.time.LocalDate
import java.time.format.DateTimeFormatter
import scala.collection.mutable

abstract class DensityFeatureBaseJob {
  val jobName = "DensityFeatureBaseJob"

  val featurePairs: List[(String, String)] = List(("AliasedSupplyPublisherId", "City"), ("Site", "Zip"))
  val featurePairStrings = featurePairs.map { case (f1, f2) => s"$f1$f2"}

  val salt = "TRM"

  val filterSensitiveAdv: mutable.HashMap[String, Boolean] = mutable.HashMap[String, Boolean](
    "AliasedSupplyPublisherIdCity" -> true,
    "SiteZip" -> false
  )

  def getDateStr(date: LocalDate): String = {
    val dtf = DateTimeFormatter.ofPattern("yyyyMMdd")
    date.format(dtf)
  }

  def readBidsImpressions(featurePairs: List[(String, String)], date: LocalDate, hour: Option[Int]) = {
    val yyyy = date.getYear.toString
    val mm = f"${date.getMonthValue}%02d"
    val dd = f"${date.getDayOfMonth}%02d"

    val colsToRead = featurePairs.flatMap { case (f1, f2) => Array(f1, f2) }.toArray.distinct

    try {
      val bidsImpsPath = s"${BidsImpressions.BIDSIMPRESSIONSS3}/prod/bidsimpressions/year=$yyyy/month=$mm/day=$dd/"

      var bidsImpressions = hour match {
        case Some(h) => spark.read.parquet(bidsImpsPath + s"hourPart=$h/")
        case _ => spark.read.parquet(bidsImpsPath)
      }

      bidsImpressions = bidsImpressions
        .select("UIID", colsToRead: _*)
        .withColumnRenamed("UIID", "TDID")
        .filter(col("TDID") =!= "00000000-0000-0000-0000-000000000000")

      featurePairs.foreach { case (feature1, feature2) =>
        bidsImpressions = bidsImpressions.withColumn(
          s"${feature1}${feature2}Hashed",
          when(
            col(feature1).isNull || col(feature2).isNull,
            lit(null)
          ).otherwise(
            xxhash64(concat(concat(col(feature1), col(feature2)), lit(salt)))
          )
        )
      }

      bidsImpressions.drop(colsToRead: _*)
    }
    catch {
      case e: Throwable =>
        println("Bad column names. Please provide a valid job config!")
        throw (e)
    }
  }

  def readPolicyTable(date: LocalDate, sources: Int*) = {
    // read the given date's RSM policy table and filter for only seeds
    spark.read.parquet(s"s3://thetradedesk-mlplatform-us-east-1/configdata/$readEnv/audience/policyTable/RSM/v=1/${getDateStr(date)}000000/")
      // filter non graph data only
      .filter(col("CrossDeviceVendorId") === lit(CrossDeviceVendor.None.id) && col("Source").isin(sources: _*))
      .select(col("SourceId").as("SeedId"), col("MappingId"), col("SyntheticId"), col("IsSensitive"))
  }

  def readAggregatedSeed(date: LocalDate, numDays: Int = 7): DataFrame = {
    for (i <- 0 until numDays) {
      val sourcePath = s"s3://thetradedesk-mlplatform-us-east-1/data/$readEnv/audience/aggregatedSeed/v=1/date=${getDateStr(date.minusDays(i))}"
      val sourceSuccessFilePath = s"${sourcePath}/_SUCCESS"

      if (FSUtils.fileExists(sourceSuccessFilePath)(spark)) {
        return spark.read.parquet(sourcePath)
      }
    }
    throw new RuntimeException("aggregated seed dataset not existing")
  }

  def runTransform(args: Array[String]): Unit

  def main(args: Array[String]): Unit = {
    runTransform(args)

    spark.catalog.clearCache()
    spark.stop()
  }

}
