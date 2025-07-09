package com.thetradedesk.audience.jobs.modelinput.rsmv2.usersampling

import com.thetradedesk.spark.TTDSparkContext.spark.implicits._
import org.apache.spark.sql.Column
import org.apache.spark.sql.expressions.UserDefinedFunction
import org.apache.spark.sql.functions._
import com.thetradedesk.audience.jobs.modelinput.rsmv2.RelevanceModelInputGeneratorJobConfig

import java.util.UUID

object SIBSampler extends Sampler {

  case class Uuid(MostSigBits: Long, LeastSigBits: Long)

  def charToLong(char: Char): Long = {
    if (char >= '0' && char <= '9')
      char - '0'
    else if (char >= 'A' && char <= 'F')
      char - ('A' - 0xaL)
    else if (char >= 'a' && char <= 'f')
      char - ('a' - 0xaL)
    else
      0L
  }

  def guidToLongs(tdid: String): Uuid = {
    if (tdid == null) {
      Uuid(0, 0)
    } else {
      val hexString = tdid.replace("-", "")

      if (!hexString.matches("[0-9A-Fa-f]{32}")) {
        Uuid(0, 0)
      } else {
        val highBits = hexString.slice(0, 16).map(charToLong).zipWithIndex.map { case (value, i) => value << (60 - (4 * i)) }.reduce(_ | _)
        val lowBits = hexString.slice(16, 32).map(charToLong).zipWithIndex.map { case (value, i) => value << (60 - (4 * i)) }.reduce(_ | _)

        Uuid(highBits, lowBits)
      }
    }
  }

  def udfGuidToLongs: UserDefinedFunction = udf((id: String) => guidToLongs(id))

  def longsToGuid(tdid1: Long, tdid2: Long): String = {
    new UUID(tdid1, tdid2).toString
  }

  def toInt32(bytes: Array[Byte], index: Int): Int = {
    val b1 = (0xff & (bytes(index) & 0xFF)) << 0
    val b2 = (0xff & (bytes(index + 1) & 0xFF)) << 8
    val b3 = (0xff & (bytes(index + 2) & 0xFF)) << 16
    val b4 = (0xff & (bytes(index + 3) & 0xFF)) << 24

    b1 | b2 | b3 | b4
  }

  /**
   * Calculate whether a given TDID is included in the 10% sample for TargetingDataUser
   * This is a replication of the logic that DataServer uses
   *
   * @return Boolean
   */
  def userIsSampled(mostSigBits: Long, leastSigBits: Long, basePopulation: Int, samplePopulation: Int): Boolean = {
    // the logic here is replicated from Constants.UserIsSampled from AdPlatform
    val tdidBytes = Array.fill[Byte](16)(0)

    // most sig bits first, then least sig bits
    // so the reason for this super wacky ordering is to account for the differences in how guids are represented
    // in binary in c# vs java. the 2 longs that make up TDIDs come from the java representation but the 10% sample
    // is done in DataServer from the c#  representation
    tdidBytes(3) = ((mostSigBits >> 56) & 0xFF).asInstanceOf[Byte]
    tdidBytes(2) = ((mostSigBits >> 48) & 0xFF).asInstanceOf[Byte]
    tdidBytes(1) = ((mostSigBits >> 40) & 0xFF).asInstanceOf[Byte]
    tdidBytes(0) = ((mostSigBits >> 32) & 0xFF).asInstanceOf[Byte]
    tdidBytes(5) = ((mostSigBits >> 24) & 0xFF).asInstanceOf[Byte]
    tdidBytes(4) = ((mostSigBits >> 16) & 0xFF).asInstanceOf[Byte]
    tdidBytes(7) = ((mostSigBits >> 8) & 0xFF).asInstanceOf[Byte]
    tdidBytes(6) = ((mostSigBits >> 0) & 0xFF).asInstanceOf[Byte]

    tdidBytes(8) = ((leastSigBits >> 56) & 0xFF).asInstanceOf[Byte]
    tdidBytes(9) = ((leastSigBits >> 48) & 0xFF).asInstanceOf[Byte]
    tdidBytes(10) = ((leastSigBits >> 40) & 0xFF).asInstanceOf[Byte]
    tdidBytes(11) = ((leastSigBits >> 32) & 0xFF).asInstanceOf[Byte]
    tdidBytes(12) = ((leastSigBits >> 24) & 0xFF).asInstanceOf[Byte]
    tdidBytes(13) = ((leastSigBits >> 16) & 0xFF).asInstanceOf[Byte]
    tdidBytes(14) = ((leastSigBits >> 8) & 0xFF).asInstanceOf[Byte]
    tdidBytes(15) = ((leastSigBits >> 0) & 0xFF).asInstanceOf[Byte]

    val total = (
      toInt32(tdidBytes, 0).asInstanceOf[Long]
        + toInt32(tdidBytes, 4)
        + toInt32(tdidBytes, 8)
        + toInt32(tdidBytes, 12)
      )

    math.abs(total % basePopulation) < samplePopulation
  }

  def _isDeviceIdSampled(deviceId: String): Boolean = {

    if (deviceId == "") {
      return false
    }

    // Parse GUID
    val guidAsLongs = guidToLongs(deviceId)

    // Must match Constants.UserIsSampled in AdPlatform.
    // Also must stay constant between runs in prod as we read datasets produced from multiple runs.
    userIsSampled(guidAsLongs.MostSigBits, guidAsLongs.LeastSigBits, 1000000, 100000)
  }

  def _isDeviceIdSampledNPercent(deviceId: String, n: Int): Boolean = {

    if ( n > 100 || n < 1 ) {
      throw new IllegalArgumentException("Sampling percentage cannot exceed 100% or be less than 1% (n must be ≤ 100 && >= 1)")
    }

    if (deviceId == "") {
      return false
    }

    // Parse GUID
    val guidAsLongs = guidToLongs(deviceId)

    // Must match Constants.UserIsSampled in AdPlatform.
    // Also must stay constant between runs in prod as we read datasets produced from multiple runs.
    userIsSampled(guidAsLongs.MostSigBits, guidAsLongs.LeastSigBits, 1000000, 10000 * n) // sample 1%
  }

  def _isDeviceIdSampled1Percent(deviceId: String): Boolean = {

    if (deviceId == "") {
      return false
    }

    // Parse GUID
    val guidAsLongs = guidToLongs(deviceId)

    // Must match Constants.UserIsSampled in AdPlatform.
    // Also must stay constant between runs in prod as we read datasets produced from multiple runs.
    userIsSampled(guidAsLongs.MostSigBits, guidAsLongs.LeastSigBits, 1000000, 10000) // sample 1%
  }

  val isDeviceIdSampled = udf(_isDeviceIdSampled _)
  val isDeviceIdSampled1Percent = udf(_isDeviceIdSampled1Percent _)

  override def samplingFunction(symbol: Symbol, conf: RelevanceModelInputGeneratorJobConfig): Column = {
    isDeviceIdSampled(symbol)
  }

}
