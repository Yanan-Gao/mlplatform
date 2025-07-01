package com.thetradedesk.audience.transform

import com.thetradedesk.audience.datasets.AggregatedSeedRecord
import com.thetradedesk.audience.transform.IDTransform.IDType.IDType
import com.thetradedesk.audience.{doNotTrackTDID, doNotTrackTDIDColumn}
import com.thetradedesk.spark.TTDSparkContext.spark.implicits._
import org.apache.spark.sql.{Column, DataFrame, Dataset}
import org.apache.spark.sql.functions._

import java.nio.ByteBuffer
import java.security.MessageDigest
import java.util.UUID
import scala.collection.mutable.ArrayBuffer

object IDTransform {

  def convertUID2ToGUID(uid2: String) = {
    try {
      val md5 = MessageDigest.getInstance("MD5")
      val hashBytes = md5.digest(uid2.getBytes("ASCII"))

      val bb = ByteBuffer.wrap(hashBytes)
      val high = bb.getLong
      val low = bb.getLong

      val uuid = new UUID(high, low)
      uuid.toString
    } catch {
      case _: Exception => null
    }
  }

  val convertUID2ToGUIDUDF = udf(convertUID2ToGUID _)


  object IDType extends Enumeration {
    type IDType = Value
    val Unknown, DeviceAdvertisingId, CookieTDID, UnifiedId2, EUID, IdentityLinkId = Value
  }

  val allIdWithTypeUDF = udf((
                               DeviceAdvertisingId: String,
                               CookieTDID: String,
                               UnifiedId2: String,
                               EUID: String,
                               IdentityLinkId: String
                             ) => {
    val buffer = new ArrayBuffer[(String, Int)](6)

    // when CookieTDID == DeviceAdvertisingId, keep the latter
    // we don't expect clash among other id types
    if (CookieTDID != null && CookieTDID != doNotTrackTDID && CookieTDID != DeviceAdvertisingId) {
      buffer.append((CookieTDID, IDType.CookieTDID.id))
    }
    if (DeviceAdvertisingId != null && DeviceAdvertisingId != doNotTrackTDID) {
      buffer.append((DeviceAdvertisingId, IDType.DeviceAdvertisingId.id))
    }
    if (UnifiedId2 != null && UnifiedId2 != doNotTrackTDID) {
      buffer.append((UnifiedId2, IDType.UnifiedId2.id))
    }
    if (EUID != null && EUID != doNotTrackTDID) {
      buffer.append((EUID, IDType.EUID.id))
    }
    if (IdentityLinkId != null && IdentityLinkId != doNotTrackTDID) {
      buffer.append((IdentityLinkId, IDType.IdentityLinkId.id))
    }
    buffer.toArray
  })

  val allIdWithType = explode(
    allIdWithTypeUDF(
      col("DeviceAdvertisingId"),
      col("CookieTDID"),
      col("UnifiedId2"),
      col("EUID"),
      col("IdentityLinkId")
    )
  )

  def joinOnIdType(df1: DataFrame, df2: DataFrame, idType: IDType, joinType: String = "inner"): DataFrame = {
    df1
      .withColumn("X", col(idType.toString))
      .drop("CookieTDID", "DeviceAdvertisingId", "UnifiedId2", "EUID", "IdentityLinkId", "TDID")
      .withColumnRenamed("X", "TDID")
      .where('TDID.isNotNull && 'TDID =!= doNotTrackTDIDColumn)
      .join(df2.where('idType === lit(idType.id)), Seq("TDID"), joinType)
  }

  def joinOnIdTypes(df1: DataFrame, df2: DataFrame, joinType: String = "inner"): DataFrame = {
    IDType.values
      .filter(_ != IDType.Unknown)
      .map(e => joinOnIdType(df1, df2, e, joinType))
      .reduce(_ union _)
  }

  def idTypesBitmap : Column =
    IDType.values.filter(_ != IDType.Unknown)
      .map { idType =>
        when(col(idType.toString).isNotNull && col(idType.toString) =!= lit(doNotTrackTDIDColumn),
          lit(1 << (idType.id - 1)))
          .otherwise(lit(0))
      }.reduce(_ bitwiseOR _)
      .cast("integer")

  val hasIdType = udf((bitmap: Int, idType: Int) => {
    val mask = 1 << (idType - 1)
    (bitmap & mask) != 0
  })

  def filterOnIdType(idType: IDType, sampleFun: Column => Column): Column = {
    sampleFun(col(idType.toString))
  }

  def filterOnIdTypes(sampleFun: Column => Column): Column = filterExclusiveIDTypes(sampleFun, IDType.Unknown)

  def filterOnIdTypesSym(sampleFun: Symbol => Column): Column = filterOnIdTypes(col => sampleFun(Symbol(col.toString())))

  def filterExclusiveIDTypes(sampleFun: Column => Column, exclusiveIDTypes: IDType*): Column = {
    IDType.values
      .filter(!exclusiveIDTypes.contains(_))
      .map(e => sampleFun(col(e.toString)))
      .reduce(_ || _)
  }

  // use to get the IDtype back
  def getExistingIdTypes(bitmap: Int): Seq[IDType.Value] = {
    IDType.values.toSeq
      .filter(_ != IDType.Unknown)
      .filter { idType =>
        val mask = 1 << (idType.id - 1)
        (bitmap & mask) != 0
      }
  }
}
