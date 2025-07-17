package com.thetradedesk.featurestore.transform

import com.thetradedesk.featurestore.{doNotTrackTDID, doNotTrackTDIDColumn}
import com.thetradedesk.featurestore.transform.IDTransform.IDType.IDType
import com.thetradedesk.spark.TTDSparkContext.spark.implicits._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{Column, DataFrame}

import java.nio.ByteBuffer
import java.security.MessageDigest
import java.util.UUID
import scala.collection.mutable.ArrayBuffer

/**
 * copied from audience project
 */
object IDTransform {
  object IDType extends Enumeration {
    type IDType = Value
    val Unknown, DeviceAdvertisingId, CookieTDID, UnifiedId2, EUID, IdentityLinkId = Value
  }

  val allIdsUDF = udf((
                        DeviceAdvertisingId: String,
                        CookieTDID: String,
                        UnifiedId2: String,
                        EUID: String,
                        IdentityLinkId: String
                      ) => {
    val buffer = new ArrayBuffer[String](6)

    // when DeviceAdvertisingId and CookieTDID are together, only keep one
    if (DeviceAdvertisingId != null && DeviceAdvertisingId != doNotTrackTDID) {
      buffer.append(DeviceAdvertisingId)
    } else if (CookieTDID != null && CookieTDID != doNotTrackTDID) {
      buffer.append(CookieTDID)
    }
    if (UnifiedId2 != null && UnifiedId2 != doNotTrackTDID) {
      buffer.append(UnifiedId2)
    }
    if (EUID != null && EUID != doNotTrackTDID) {
      buffer.append(EUID)
    }
    if (IdentityLinkId != null && IdentityLinkId != doNotTrackTDID) {
      buffer.append(IdentityLinkId)
    }
    buffer.toArray
  })

  val allIdTypes = Seq("DeviceAdvertisingId", "CookieTDID", "UnifiedId2", "EUID", "IdentityLinkId")

  val allIdType = explode(allIdsUDF(allIdTypes.map(col):_*))

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

  def filterOnIdType(idType: IDType, sampleFun: Column => Column): Column = {
    sampleFun(col(idType.toString))
  }

  def filterOnIdTypes(sampleFun: Column => Column): Column = filterExclusiveIDTypes(sampleFun, IDType.Unknown)

  def filterExclusiveIDTypes(sampleFun: Column => Column, exclusiveIDTypes: IDType*): Column = {
    IDType.values
      .filter(!exclusiveIDTypes.contains(_))
      .map(e => sampleFun(col(e.toString)))
      .reduce(_ || _)
  }
}