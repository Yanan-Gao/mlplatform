package com.thetradedesk.kongming.datasets

import com.thetradedesk.kongming.IdentityHouseholdUnmatchedToken
import org.apache.hadoop.fs.{FileSystem, Path}
import com.thetradedesk.spark.sql.SQLFunctions._
import org.apache.spark.sql.functions._

import java.net.URI
import java.time.format.{DateTimeFormatter, DateTimeParseException}
import com.thetradedesk.spark.TTDSparkContext.spark
import org.apache.spark.sql.Dataset
import com.thetradedesk.spark.TTDSparkContext.spark.implicits._

import java.time.LocalDate

final case class CrossDeviceGraphRecord(
                                         HouseholdID:String,
                                         PersonID:String,
                                         uiid:String,
                                         score:Double
                                       )
final case class IDRecord(uiid:String)

final case class PersonRecord(PersonId: String,
                              IdScoreMap: Map[String, Double])

object CrossDeviceGraphDataset {

  def getLocalDateFromString(str: String): LocalDate = {
    try {
      LocalDate.parse(str)
    } catch {
      case _: DateTimeParseException => LocalDate.parse("2000-01-01")
    }
  }

  def loadGraph(date:LocalDate, scoreThreshold:Double=0.0, graphname:String="iav2graph") : Dataset[CrossDeviceGraphRecord] ={
    val crossDeviceVendorLocation = "s3a://thetradedesk-useast-data-import/sxd-etl/universal/"+graphname
    val availableDates = FileSystem.get(new URI(crossDeviceVendorLocation), spark.sparkContext.hadoopConfiguration)
      .listStatus(new Path(crossDeviceVendorLocation))
      .map(_.getPath.getName)
      .map(getLocalDateFromString)

    val latestDate = availableDates.filter(_.isBefore(date)).map(_.format(DateTimeFormatter.ofPattern("yyyy-MM-dd"))).max

    spark
      .read
      .parquet(crossDeviceVendorLocation + "/" + latestDate + "/success")
      .selectAs[CrossDeviceGraphRecord]
      .filter($"score">lit(scoreThreshold))
  }

  def shrinkGraph(parentGraph: Dataset[CrossDeviceGraphRecord], idDS:Dataset[IDRecord]): Dataset[CrossDeviceGraphRecord] = {
    // this function is taking in the raw adbrain graph and IDs we observe in conversion data and shrink it to a smaller size
    // personDeviceDS: <personId, uiid>

    val unifiedKeyGraph = parentGraph.withColumn("JoinKey", when($"HouseholdID"=!=lit(IdentityHouseholdUnmatchedToken), $"HouseholdID").otherwise($"PersonID"))
    val personDS = unifiedKeyGraph.join(idDS, Seq("uiid"),"inner").select($"JoinKey").distinct
    val personDeviceDS = unifiedKeyGraph.join(personDS, Seq("JoinKey"),"inner").selectAs[CrossDeviceGraphRecord].cache

    personDeviceDS
  }
}
