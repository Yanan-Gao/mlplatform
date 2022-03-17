package com.ttd.mycellium.pipelines.conversiontracker

import com.ttd.features.transformers.{DropNA, Filter, Select, WithColumn}
import org.apache.spark.ml.Pipeline
import org.apache.spark.sql.functions.{col, unix_timestamp}

class ConversionTrackerInputPipe extends Pipeline {
  setStages(Array(
    Filter(col("TDID") =!= "00000000-0000-0000-0000-000000000000"),
    Select("LogEntryTime", "TDID",
      "ReferrerUrl", "CampaignId", "AdvertiserId", "ConversionType"
    ),
    // drop if can't associate with any ID
    DropNA(Array("TDID")),
    WithColumn("ts", unix_timestamp(col("LogEntryTime"))),
  ))
}
