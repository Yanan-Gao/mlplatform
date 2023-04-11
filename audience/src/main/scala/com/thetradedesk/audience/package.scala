package com.thetradedesk

import com.thetradedesk.spark.TTDSparkContext.spark.implicits._
import com.thetradedesk.spark.sql.SQLFunctions._
import com.thetradedesk.spark.util.HashingUtils.userIsInSample
import com.thetradedesk.spark.util.TTDConfig.config

import org.apache.spark.sql.functions._

import java.time.LocalDate

package object audience {
  var date = config.getDate("date" , LocalDate.now())
  var ttdEnv = config.getString("ttd.env" , "dev")
  val trainSetDownSampleFactor = config.getInt("trainSetDownSampleFactor", default = 2)
  val sampleHit = config.getString("sampleHit", "0")

  val userDownSampleBasePopulation = config.getInt("userDownSampleBasePopulation", default = 1000000)
  val userDownSampleHitPopulation = config.getInt("userDownSampleHitPopulation", default = 10000)
  val userDownSampleHitPopulationV2 = config.getInt("userDownSampleHitPopulationV2", default = 100000)

  private val userIsInSampleUDF = udf[Boolean, String, Long, Long](userIsInSample)
  private val doNotTrackTDID = lit("00000000-0000-0000-0000-000000000000")

  def shouldConsiderTDID(symbol: Symbol) = {
    symbol.isNotNullOrEmpty && symbol =!= doNotTrackTDID && substring(symbol, 9, 1) === lit("-") && userIsInSampleUDF(symbol, lit(userDownSampleBasePopulation), lit(userDownSampleHitPopulation))
  }

  def shouldConsiderTDID2(symbol: Symbol) = {
    symbol.isNotNullOrEmpty && symbol =!= doNotTrackTDID && substring(symbol, 9, 1) === lit("-") && userIsInSampleUDF(symbol, lit(userDownSampleBasePopulation), lit(userDownSampleHitPopulationV2))
  }

  def shouldConsiderTDID3(symbol: Symbol, userDownSampleHitPopulation: Int) = {
    symbol.isNotNullOrEmpty && symbol =!= doNotTrackTDID && substring(symbol, 9, 1) === lit("-") && userIsInSampleUDF(symbol, lit(userDownSampleBasePopulation), lit(userDownSampleHitPopulation))
  }
}
