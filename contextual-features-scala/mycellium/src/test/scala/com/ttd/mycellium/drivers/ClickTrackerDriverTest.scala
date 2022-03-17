package com.ttd.mycellium.drivers

import com.github.mrpowers.spark.fast.tests.DatasetComparer
import com.ttd.features.datasets.ReadableDataFrame
import com.ttd.features.util.ReadableDataFrameUtils._
import com.ttd.mycellium.util.{TTDSparkTest, TempPathUtils}
import org.apache.spark.sql.SaveMode
import org.apache.spark.sql.functions.{current_timestamp, lit}

class ClickTrackerDriverTest extends TTDSparkTest with DatasetComparer {
  test("ClickTracker driver can run") {
    val df = spark.createDataFrame(Seq(
      ("TDID", "DeviceAdvertisingId", "UnifiedId2",
        "RedirectUrl", "ReferrerUrl", "SupplyVendor", "SupplyVendorPublisherId", "Site",
        "AdGroupId", "CampaignId", "CreativeId", "AdvertiserId", "PartnerId", "ChannelId", "IdentityLinkId"
      )
    )).toDF("TDID", "DeviceAdvertisingId", "UnifiedId2",
      "RedirectUrl", "ReferrerUrl", "SupplyVendor", "SupplyVendorPublisherId", "Site",
      "AdGroupId", "CampaignId", "CreativeId", "AdvertiserId", "PartnerId", "ChannelId", "IdentityLinkId"
    )
      .withColumn("LogEntryTime", current_timestamp())

    TempPathUtils.withTempDir(path => {
      new ClickTrackerDriver(new ClickTrackerConfig {
        override val saveMode: SaveMode = SaveMode.Overwrite
        override val clickTracker: ReadableDataFrame = df.asReadableDataFrame
        override val outputBasePath: String = path
      }).run()
    })
  }
}
