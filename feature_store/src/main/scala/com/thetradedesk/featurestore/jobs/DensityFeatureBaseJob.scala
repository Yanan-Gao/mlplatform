package com.thetradedesk.featurestore.jobs

import com.thetradedesk.featurestore._
import com.thetradedesk.featurestore.rsm.CommonEnums.{CrossDeviceVendor, DataSource}
import com.thetradedesk.featurestore.transform.IDTransform.{allIdType, filterOnIdTypes}
import com.thetradedesk.geronimo.bidsimpression.schema.BidsImpressions
import com.thetradedesk.spark.TTDSparkContext.spark
import com.thetradedesk.spark.TTDSparkContext.spark.implicits._
import com.thetradedesk.spark.util.io.FSUtils
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{DataFrame, SaveMode}

import java.time.LocalDate
import java.time.format.DateTimeFormatter
import scala.collection.mutable

abstract class DensityFeatureBaseJob {
  val jobName = "DensityFeatureBaseJob"

  val nonSensitiveFeaturePair = "SiteZip"
  val sensitiveFeaturePair = "AliasedSupplyPublisherIdCity"
  val featurePairs: List[(String, String)] = List(("AliasedSupplyPublisherId", "City"), ("Site", "Zip"))

  val featurePairStrings = featurePairs.map { case (f1, f2) => s"$f1$f2"}

  val salt = "TRM"

  val filterSensitiveAdv: mutable.HashMap[String, Boolean] = mutable.HashMap[String, Boolean](
    sensitiveFeaturePair -> true,
    nonSensitiveFeaturePair -> false
  )

  def getDateStr(date: LocalDate): String = {
    val dtf = DateTimeFormatter.ofPattern("yyyyMMdd")
    date.format(dtf)
  }

  def readBidsImpressionsWithIDExploded(featurePairs: List[(String, String)], date: LocalDate, hour: Option[Int]) = {
    readBidsImpressions(featurePairs, date, hour)
      .withColumn("TDID", allIdType)
      .select("TDID", (featurePairs.map(e => s"${e._1}${e._2}Hashed") :+ "BidRequestId"): _*)
  }

  def readBidsImpressions(featurePairs: List[(String, String)], date: LocalDate, hour: Option[Int]) = {
    val yyyy = date.getYear.toString
    val mm = f"${date.getMonthValue}%02d"
    val dd = f"${date.getDayOfMonth}%02d"

    val colsToRead = featurePairs.flatMap { case (f1, f2) => Array(f1, f2) }.toArray.distinct
    val userWindowSpec = Window.partitionBy("UIID")
    val hourUserWindowSpec = Window.partitionBy("UIID", "hourPart")

    try {
      val bidsImpsPath = s"${BidsImpressions.BIDSIMPRESSIONSS3}/prod/bidsimpressions/year=$yyyy/month=$mm/day=$dd/"

      var bidsImpressions = hour match {
        case Some(h) => spark.read.parquet(bidsImpsPath + s"hourPart=$h/")
          .withColumn("UserFrequency", count("*").over(userWindowSpec))
          .where('UserFrequency < lit(normalUserBidCountPerHour))
          .drop('UserFrequency)
        case _ => spark.read.parquet(bidsImpsPath)
          .withColumn("UserFrequency", count("*").over(hourUserWindowSpec))
          .where('UserFrequency < lit(normalUserBidCountPerHour))
          .drop('UserFrequency)
      }

      val emptyFilter = featurePairs.map(e => (col(e._1).isNotNull && col(e._2).isNotNull)).reduce(_ || _)

      bidsImpressions = bidsImpressions
        .where(emptyFilter)
        .where(filterOnIdTypes(shouldTrackTDID))

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

      val colsToKeep = colsToRead ++ featurePairs.map(e => s"${e._1}${e._2}Hashed") :+ "DeviceAdvertisingId" :+ "CookieTDID" :+ "UnifiedId2" :+ "EUID" :+ "IdentityLinkId"

      bidsImpressions
        .select("BidRequestId", colsToKeep : _*)
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
      .select(col("SourceId").as("SeedId"), col("MappingId"), col("SyntheticId"), col("IsSensitive"), 'Source)
  }

  def readAggregatedSeed(date: LocalDate): DataFrame = {
    spark.read.parquet(s"s3://thetradedesk-mlplatform-us-east-1/data/$readEnv/audience/aggregatedSeed/v=1/date=${getDateStr(date)}")
  }

  def runTransform(args: Array[String]): Unit

  def main(args: Array[String]): Unit = {
    runTransform(args)

    spark.catalog.clearCache()
    spark.stop()
  }

}
