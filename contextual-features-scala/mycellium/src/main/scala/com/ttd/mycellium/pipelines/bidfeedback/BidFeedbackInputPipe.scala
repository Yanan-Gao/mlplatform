package com.ttd.mycellium.pipelines.bidfeedback

import com.ttd.features.transformers.{DropNA, Filter, Select, WithColumn}
import org.apache.spark.ml.Pipeline
import org.apache.spark.sql.functions.{col, unix_timestamp}

class BidFeedbackInputPipe extends Pipeline {
  setStages(Array(
    Filter(col("TDID") =!= "00000000-0000-0000-0000-000000000000"),
    Select("LogEntryTime", "TDID", "DeviceAdvertisingId", "UnifiedId2",
      "RawUrl", "SupplyVendor", "AdGroupId", "CampaignId", "CreativeId",
      "AdvertiserId", "PartnerId", "ChannelId", "Site"
    ),
    // drop if can't associate with any ID
    DropNA(Array("TDID", "DeviceAdvertisingId"), "all"),
    WithColumn("ts", unix_timestamp(col("LogEntryTime"))),
  ))
}
