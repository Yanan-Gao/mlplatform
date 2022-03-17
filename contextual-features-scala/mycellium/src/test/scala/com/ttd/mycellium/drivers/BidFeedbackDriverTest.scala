package com.ttd.mycellium.drivers

import com.github.mrpowers.spark.fast.tests.DatasetComparer
import com.ttd.features.datasets.ReadableDataFrame
import com.ttd.features.util.ReadableDataFrameUtils._
import com.ttd.mycellium.util.{TTDSparkTest, TempPathUtils}
import org.apache.spark.sql.SaveMode
import org.apache.spark.sql.functions.{current_timestamp, lit, from_json}

class BidFeedbackDriverTest extends TTDSparkTest with DatasetComparer {
  test("BidFeedback driver can run") {
    val df = spark.createDataFrame(Seq(
      ("TDID", "DeviceAdvertisingId", "UnifiedId2",
        "RawUrl", "SupplyVendor", "AdGroupId", "CampaignId", "CreativeId",
        "AdvertiserId", "PartnerId", "ChannelId", "Site")
    )).toDF("TDID", "DeviceAdvertisingId", "UnifiedId2",
      "RawUrl", "SupplyVendor", "AdGroupId", "CampaignId", "CreativeId",
      "AdvertiserId", "PartnerId", "ChannelId", "Site")
      .withColumn("Region", lit("Region"))
      .withColumn("LogEntryTime", current_timestamp())

    TempPathUtils.withTempDir(path => {
      new BidFeedbackDriver(new BidFeedbackConfig {
        override val saveMode: SaveMode = SaveMode.Overwrite
        override val bidFeedback: ReadableDataFrame = df.asReadableDataFrame
        override val outputBasePath: String = path
      }).run()
    })
  }
}
