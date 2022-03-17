package com.ttd.mycellium.pipelines.bidrequest

import com.ttd.features.transformers.{DropNA, Filter, Select, WithColumn}
import org.apache.spark.ml.Pipeline
import org.apache.spark.sql.functions.{col, unix_timestamp}

class BidRequestInputPipe extends Pipeline {
  setStages(Array(
    Filter(col("TDID") =!= "00000000-0000-0000-0000-000000000000"),
    Filter(col("TDID").isNotNull || col("DeviceAdvertisingId").isNotNull),
    Select("LogEntryTime", "IPAddress", "DeviceType", "TDID", "OperatingSystem", "Browser",
      "OperatingSystemFamily", "DeviceAdvertisingId", "DeviceMake", "DeviceModel",
      "AppStoreUrl", "Site", "RawUrl", "ReferrerUrl", "SupplyVendor", "SupplyVendorPublisherId", "SupplyVendorSiteId",
      "AdGroupId", "CampaignId", "CreativeId", "AdvertiserId", "PartnerId",
      "Country", "Region", "City", "Zip", "UnifiedId2", "IdentityLinkId",
      "Latitude", "Longitude"
    ),
    // drop if can't associate with any ID
    WithColumn("ts", unix_timestamp(col("LogEntryTime"))),
  ))
}
