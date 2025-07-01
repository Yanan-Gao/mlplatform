package com.thetradedesk.audience.transform

import com.thetradedesk.audience.{doNotTrackTDID, doNotTrackTDIDColumn}
import com.thetradedesk.audience.transform.IDTransform.{IDType, hasIdType, idTypesBitmap, joinOnIdTypes}
import com.thetradedesk.audience.utils.TTDSparkTest
import org.apache.spark.sql.functions.{col, lit}

class IDTransformTest extends TTDSparkTest {
  test("test joinOnIdType inner join") {
    val df1 = spark.createDataFrame(Seq(
      ("1", "device1", "cookie1", null, null, null),
      ("2", "device2", doNotTrackTDID, "uid2a", null, null),
      ("3", "device3", null, null, null, null),
      ("4", "device4", null, null, null, null)
    )).toDF("BidRequestId", "DeviceAdvertisingId", "CookieTDID", "UnifiedId2", "EUID", "IdentityLinkId")

    val df2 = spark.createDataFrame(Seq(
      ("1", "cookie1", Some(IDType.CookieTDID.id)),
      ("2", "uid2a", Some(IDType.UnifiedId2.id)),
      ("3", "device1", Some(IDType.DeviceAdvertisingId.id)),
      ("4", "device4",  Some(IDType.DeviceAdvertisingId.id))
    )).toDF("SeedId", "TDID", "idType")

    val actualDf = joinOnIdTypes(df1, df2, "inner")

    val expectedDf = spark.createDataFrame(Seq(
      ("uid2a", "2", "2", Some(IDType.UnifiedId2.id)),
      ("cookie1", "1", "1", Some(IDType.CookieTDID.id)),
      ("device1", "1", "3", Some(IDType.DeviceAdvertisingId.id)),
      ("device4", "4", "4",  Some(IDType.DeviceAdvertisingId.id))
    )).toDF("TDID", "BidRequestId", "SeedId", "idType")

    assert(actualDf.collect().toSet == expectedDf.collect().toSet)
  }

  test("test joinOnIdType left join") {
    val df1 = spark.createDataFrame(Seq(
      ("1", "device1", "cookie1", null, null, null),
      ("2", "device2", doNotTrackTDID, "uid2a", null, null),
      ("3", "device3", null, null, null, null),
      ("4", "device4", null, null, null, null),
      ("5", "device5", null, null, null, null)
    )).toDF("BidRequestId", "DeviceAdvertisingId", "CookieTDID", "UnifiedId2", "EUID", "IdentityLinkId")

    val df2 = spark.createDataFrame(Seq(
      ("1", "cookie1", Some(IDType.CookieTDID.id)),
      ("2", "uid2a", Some(IDType.UnifiedId2.id)),
      ("3", "device1", Some(IDType.DeviceAdvertisingId.id)),
      ("4", "device4", Some(IDType.DeviceAdvertisingId.id))
    )).toDF("SeedId", "TDID", "idType")

    val actualDf = joinOnIdTypes(df1, df2, "left").select("TDID", "BidRequestId", "SeedId", "idType")

    val expectedDf = spark.createDataFrame(Seq(
      ("cookie1", "1", "1", Some(IDType.CookieTDID.id)),
      ("device1", "1", "3", Some(IDType.DeviceAdvertisingId.id)),
      ("uid2a", "2", "2", Some(IDType.UnifiedId2.id)),
      ("device2", "2", null, null),
      ("device3", "3", null, null),
      ("device4", "4", "4",  Some(IDType.DeviceAdvertisingId.id)),
      ("device5", "5", null, null)
    )).toDF("TDID", "BidRequestId", "SeedId", "idType")

    assert(actualDf.collect().toSet == expectedDf.collect().toSet)
  }

  test("test ID types bitmap") {
    var df = spark.createDataFrame(Seq(
      ("device1", null, "uid2a", null, doNotTrackTDID),
      ("device2", "cookie2", null, "euid1", null)
    ))
      .toDF("DeviceAdvertisingId", "CookieTDID", "UnifiedId2", "EUID", "IdentityLinkId")
      .withColumn("IdTypesBitmap", idTypesBitmap)
      .withColumn("hasDeviceAdvertisingId", hasIdType(col("IdTypesBitmap"), lit(IDType.DeviceAdvertisingId.id)))
      .withColumn("hasUnifiedId2", hasIdType(col("IdTypesBitmap"), lit(IDType.UnifiedId2.id)))
      .withColumn("hasCookieTDID", hasIdType(col("IdTypesBitmap"), lit(IDType.CookieTDID.id)))
      .withColumn("hasIdentityLinkId", hasIdType(col("IdTypesBitmap"), lit(IDType.IdentityLinkId.id)))

    val dfRows = df.collect();

    assert(dfRows(0).getAs[Boolean]("hasDeviceAdvertisingId"))
    assert(dfRows(0).getAs[Boolean]("hasUnifiedId2"))
    assert(!dfRows(0).getAs[Boolean]("hasCookieTDID"))
    assert(!dfRows(0).getAs[Boolean]("hasIdentityLinkId"))

    assert(dfRows(1).getAs[Boolean]("hasDeviceAdvertisingId"))
    assert(!dfRows(1).getAs[Boolean]("hasUnifiedId2"))
    assert(dfRows(1).getAs[Boolean]("hasCookieTDID"))
  }
}
