package com.thetradedesk

import com.thetradedesk.spark.TTDSparkContext.spark.implicits._
import com.thetradedesk.spark.sql.SQLFunctions._
import com.thetradedesk.spark.util.HashingUtils.userIsInSample
import com.thetradedesk.spark.util.TTDConfig.config
import com.amazonaws.regions.Regions
import com.amazonaws.services.s3.AmazonS3ClientBuilder
import org.apache.spark.sql.Column
import org.apache.spark.sql.catalyst.expressions.XxHash64Function
import org.apache.spark.sql.functions._
import org.apache.spark.unsafe.types.UTF8String

import java.time.{LocalDate, LocalDateTime}

package object audience {
  var date = config.getDate("date", LocalDate.now())
  var dateTime = config.getDateTime("dateTime", date.atStartOfDay())
  var ttdEnv = config.getString("ttd.env", "dev")
  val trainSetDownSampleFactor = config.getInt("trainSetDownSampleFactor", default = 2)
  val sampleHit = config.getString("sampleHit", "0")
  val s3Client = AmazonS3ClientBuilder.standard.withRegion(Regions.US_EAST_1).build

  val userDownSampleBasePopulation = config.getInt("userDownSampleBasePopulation", default = 1000000)
  val userDownSampleHitPopulation = config.getInt("userDownSampleHitPopulation", default = 10000)
  val userDownSampleHitPopulationV2 = config.getInt("userDownSampleHitPopulationV2", default = 100000)

  val audienceResultCoalesce = config.getInt("audienceResultCoalesce", 256)
  val policyTableResultCoalesce = config.getInt("policyTableResultCoalesce", 8)

  val audienceVersionDateFormat = "yyyyMMddHHmmss"

  val userIsInSampleUDF = udf[Boolean, String, Long, Long](userIsInSample)
  private val doNotTrackTDID = "00000000-0000-0000-0000-000000000000"
  private val doNotTrackTDIDColumn = lit("00000000-0000-0000-0000-000000000000")

  val seedCoalesceAfterFilter = config.getInt("seedCoalesceAfterFilter", 4)

  val dryRun = config.getBoolean("dryRun", false)

  val featuresJsonPath = config.getString("featuresJsonPath", default = "s3://thetradedesk-mlplatform-us-east-1/features/data/RSM/v=1/prod/schemas/features.json")

  def shouldConsiderTDID(symbol: Symbol) = {
    shouldTrackTDID(symbol) && substring(symbol, 9, 1) === lit("-") && userIsInSampleUDF(symbol, lit(userDownSampleBasePopulation), lit(userDownSampleHitPopulation))
  }

  def shouldConsiderTDID2(symbol: Symbol) = {
    shouldTrackTDID(symbol) && substring(symbol, 9, 1) === lit("-") && userIsInSampleUDF(symbol, lit(userDownSampleBasePopulation), lit(userDownSampleHitPopulationV2))
  }

  def shouldConsiderTDID3(userDownSampleHitPopulation: Int, salt: String)(symbol: Symbol) = {
    shouldTrackTDID(symbol) && substring(symbol, 9, 1) === lit("-") && (abs(xxhash64(concat(symbol, lit(salt)))) % lit(userDownSampleBasePopulation) < lit(userDownSampleHitPopulation))
  }

  def shouldConsiderTDIDInArray3(userDownSampleHitPopulation: Int, salt: String)
  = udf((TDIDs: Seq[String]) => {
    TDIDs.filter(e =>
      e != null
        && e.length > 10
        && e != doNotTrackTDID
        && e.charAt(8) == '-'
        && math.abs(xxhash64Function(e + salt)) % userDownSampleBasePopulation < userDownSampleHitPopulation
    )
  })

  private def xxhash64Function(str: String): Long = {
    XxHash64Function.hash(str.getBytes(), null, 42L)
  }

  def shouldTrackTDID(symbol: Symbol): Column = {
    symbol.isNotNullOrEmpty && symbol =!= doNotTrackTDIDColumn
  }
}
