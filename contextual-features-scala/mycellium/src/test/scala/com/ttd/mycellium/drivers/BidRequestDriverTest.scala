package com.ttd.mycellium.drivers

import com.github.mrpowers.spark.fast.tests.DatasetComparer
import com.ttd.features.datasets.ReadableDataFrame
import com.ttd.features.util.ReadableDataFrameUtils._
import com.ttd.mycellium.util.{TTDSparkTest, TempPathUtils}
import org.apache.spark.sql.SaveMode
import org.apache.spark.sql.functions.{current_timestamp, lit, struct}

class BidRequestDriverTest extends TTDSparkTest with DatasetComparer {
  test("BidRequest driver can run") {
    val df = spark.createDataFrame(Seq(
      ("IPAddress", "TDID", "DeviceAdvertisingId", "DeviceMake", "DeviceModel",
        "AppStoreUrl", "Site", "RawUrl", "ReferrerUrl", "SupplyVendor", "SupplyVendorPublisherId", "SupplyVendorSiteId",
        "AdGroupId", "CampaignId", "CreativeId", "AdvertiserId", "PartnerId", "Country")
    )).toDF("IPAddress", "TDID", "DeviceAdvertisingId", "DeviceMake", "DeviceModel",
      "AppStoreUrl", "Site", "RawUrl", "ReferrerUrl", "SupplyVendor", "SupplyVendorPublisherId", "SupplyVendorSiteId",
      "AdGroupId", "CampaignId", "CreativeId", "AdvertiserId", "PartnerId", "Country"
    ).withColumn("Region", lit("Region"))
      .withColumn("LogEntryTime", current_timestamp())
      .withColumn("Metro", lit("Metro"))
      .withColumn("City", lit("City"))
      .withColumn("Zip", lit("Zip"))
      .withColumn("UnifiedId2", lit("UnifiedId2"))
      .withColumn("IdentityLinkId", lit("IdentityLinkId"))
      .withColumn("Latitude", lit("Latitude"))
      .withColumn("Longitude", lit("Longitude"))
      .withColumn("Browser", struct(lit(1) as "value"))
      .withColumn("DeviceType", struct(lit(1) as "value"))
      .withColumn("OperatingSystem", struct(lit(1) as "value"))
      .withColumn("OperatingSystemFamily", struct(lit(1) as "value"))

    TempPathUtils.withTempDir(path => {
      new BidRequestDriver(new BidRequestConfig {
        override val saveMode: SaveMode = SaveMode.Overwrite
        override val bidRequest: ReadableDataFrame = df.asReadableDataFrame
        override val outputBasePath: String = path
      }).run()
    })
  }
}
