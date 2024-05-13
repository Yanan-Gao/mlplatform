package com.thetradedesk

import com.amazonaws.regions.Regions
import com.amazonaws.services.s3.AmazonS3ClientBuilder
import com.thetradedesk.spark.util.TTDConfig.config
import org.apache.spark.sql.Column
import org.apache.spark.sql.catalyst.expressions.XxHash64Function
import com.thetradedesk.spark.sql.SQLFunctions._
import com.thetradedesk.spark.util.HashingUtils.userIsInSample
import org.apache.spark.sql.functions._
import com.thetradedesk.spark.TTDSparkContext.spark.implicits._

import java.time.{LocalDate, LocalDateTime, ZoneOffset}

package object featurestore {
  var date = config.getDate("date", LocalDate.now())
  var dateTime = config.getDateTime("dateTime", date.atStartOfDay())
  var ttdEnv = config.getString("ttd.env", "dev")
  val s3Client = AmazonS3ClientBuilder.standard.withRegion(Regions.US_EAST_1).build

  val userDownSampleBasePopulation = config.getInt("userDownSampleBasePopulation", default = 1000000)
  val userDownSampleHitPopulation = config.getInt("userDownSampleHitPopulation", default = 10000)

  val userIsInSampleUDF = udf[Boolean, String, Long, Long](userIsInSample)

  private val doNotTrackTDID = "00000000-0000-0000-0000-000000000000"
  private val doNotTrackTDIDColumn = lit(doNotTrackTDID)

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
}