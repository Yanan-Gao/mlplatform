package com.thetradedesk

import com.amazonaws.regions.Regions
import com.amazonaws.services.s3.AmazonS3ClientBuilder
import com.thetradedesk.spark.sql.SQLFunctions._
import com.thetradedesk.spark.util.HashingUtils.userIsInSample
import com.thetradedesk.spark.util.TTDConfig.config
import org.apache.spark.sql.Column
import org.apache.spark.sql.catalyst.expressions.XxHash64Function
import org.apache.spark.sql.functions._

import java.time.{LocalDate, LocalDateTime, ZoneOffset}

package object featurestore {

  val FeautureAppName = "OfflineFeatureStore"
  val RunTimeGaugeName = "run_time_seconds"
  val OutputRowCountGaugeName = "output_rows_written"
  // S3 path
  val ProvisioningDatasetS3Root = "s3://thetradedesk-useast-qubole/warehouse.external/thetradedesk.db/provisioning"
  val MLPlatformS3Root: String = "s3a://thetradedesk-mlplatform-us-east-1/features/feature_store"
  val ProfileDataBasePath: String = "profiles"
  val ProcessedDataBasePath: String = "processed"

  var defaultNumPartitions: Int = config.getInt("numPartitions", default = 10)
  var date: LocalDate = config.getDate("date", LocalDate.now())
  var hourArray: Array[Int] = config.getString("hour", default = "0").split(",").map(e => e.toInt)
  var splitIndex: Array[Int] = config.getString("splitIndex", "0").split(",").map(_.toInt)
  var dateTime: LocalDateTime = config.getDateTime("dateTime", date.atStartOfDay())
  var ttdEnv: String = config.getString("ttd.env", "dev")
  var readEnv: String = config.getString("readEnv", "prod")
  val aggLevel: String = config.getString("aggLevel", "UIID")
  val writeThroughHdfs: Boolean = config.getBoolean("writeThroughHdfs", true)
  val densityFeatureWindowSizeDays = config.getInt("DensityFeatureWindowSizeDays", default = 1)

  val normalUserBidCountPerHour = config.getInt("normalUserBidCountPerHour", default = 3000)
  val overrideOutput = config.getBoolean("overrideOutput", default = false)
  val s3Client = AmazonS3ClientBuilder.standard.withRegion(Regions.US_EAST_1).build

  val userDownSampleBasePopulation = config.getInt("userDownSampleBasePopulation", default = 1000000)
  val userDownSampleHitPopulation = config.getInt("userDownSampleHitPopulation", default = 10000)

  val campaignFlightStartingBufferInDays = config.getInt("campaignFlightStartingBufferInDays", 14)
  val newSeedBufferInDays = config.getInt("newSeedBufferInDays", 7)

  val userIsInSampleUDF = udf[Boolean, String, Long, Long](userIsInSample)

  val highDimLevels = Set("UIID", "TDID")
  val isProfileHighDim: Boolean = highDimLevels.contains(aggLevel)

  val doNotTrackTDID = "00000000-0000-0000-0000-000000000000"
  val doNotTrackTDIDColumn = lit(doNotTrackTDID)

  def shouldConsiderTDID(column: Column) = {
    shouldTrackTDID(column) && substring(column, 9, 1) === lit("-") && userIsInSampleUDF(column, lit(userDownSampleBasePopulation), lit(userDownSampleHitPopulation))
  }

  def shouldConsiderTDID3(userDownSampleHitPopulation: Int, salt: String)(column: Column) = {
    shouldTrackTDID(column) && substring(column, 9, 1) === lit("-") && (abs(xxhash64(concat(column, lit(salt)))) % lit(userDownSampleBasePopulation) < lit(userDownSampleHitPopulation))
  }

  private def xxhash64Function(str: String): Long = {
    XxHash64Function.hash(str.getBytes(), null, 42L)
  }

  def shouldTrackTDID(column: Column): Column = {
    column.isNotNullOrEmpty && column =!= doNotTrackTDIDColumn
  }

  def shouldConsiderTDID(column: Column, rotation: Int, salt: String, rotationUnit: Int): Column = {
    if (rotation <= 1) {
      shouldTrackTDID(column) && substring(column, 9, 1) === lit("-")
    } else {
      val hit = dateTime.toEpochSecond(ZoneOffset.UTC) / rotationUnit % rotation
      shouldTrackTDID(column) && substring(column, 9, 1) === lit("-") && (abs(xxhash64(concat(column, lit(salt)))) % lit(rotation) === lit(hit))
    }
  }

  // partition config
  class PartitionCount {
    var DailyAttribution = 50
    var DailyClickBidFeedback = 4000
    var DailyConvertedImpressions = 5
    var AggResult = 5
  }
  val partCount = aggLevel match {
    case "UIID" => new PartitionCount
    case _ => {
      val partCount = new PartitionCount
      partCount.AggResult = 1
      partCount
    }
  }

}