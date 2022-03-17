package com.ttd.mycellium.drivers

import com.github.mrpowers.spark.fast.tests.DatasetComparer
import com.ttd.features.datasets.ReadableDataFrame
import com.ttd.features.util.ReadableDataFrameUtils._
import com.ttd.mycellium.util.{TTDSparkTest, TempPathUtils}
import org.apache.spark.sql.SaveMode
import org.apache.spark.sql.functions.current_timestamp

class ConversionTrackerDriverTest extends TTDSparkTest with DatasetComparer {
  test("ConversionTracker driver can run") {
    val df = spark.createDataFrame(Seq(
      ("TDID",
        "ReferrerUrl", "CampaignId", "AdvertiserId", "ConversionType"
      )
    )).toDF("TDID",
      "ReferrerUrl", "CampaignId", "AdvertiserId", "ConversionType"
    )
      .withColumn("LogEntryTime", current_timestamp())

    TempPathUtils.withTempDir(path => {
      new ConversionTrackerDriver(new ConversionTrackerConfig {
        override val saveMode: SaveMode = SaveMode.Overwrite
        override val conversionTracker: ReadableDataFrame = df.asReadableDataFrame
        override val outputBasePath: String = path
      }).run()
    })
  }
}
