package com.thetradedesk.audience.datasets

import com.thetradedesk.audience.datasets.S3Roots.ML_PLATFORM_ROOT
import com.thetradedesk.audience.{seedCoalesceAfterFilter, ttdWriteEnv, getClassName, audienceVersionDateFormat, audienceResultCoalesce}
import java.sql.Date
import com.thetradedesk.spark.util.TTDConfig.config

final case class RelevanceOnlineRecord(
        SupplyVendor: Option[Int],
        SupplyVendorPublisherId: Option[Int],
        AliasedSupplyPublisherId: Option[Int],
        Site: Option[Int],
        Country: Option[Int],
        Region: Option[Int],
        City: Option[Int],
        Zip: Option[Int],
        DeviceMake: Option[Int],
        DeviceModel: Option[Int],
        RequestLanguages: Int,
        RenderingContext: Option[Int],
        DeviceType: Option[Int],
        OperatingSystemFamily: Option[Int],
        OperatingSystem: Option[Int],
        MatchedFoldPosition: Option[Int],
        InternetConnectionType: Option[Int],
        Browser: Option[Int],
        ContextualCategoriesTier1: Array[Int],
        MatchedSegments: Array[Long],
        MatchedSegmentsLength: Double,
        HasMatchedSegments: Option[Int],
        UserSegmentCount: Double,
        AdWidthInPixels: Double,
        AdHeightInPixels: Double,

        sin_hour_week: Double,
        cos_hour_week: Double,
        sin_hour_day: Double,
        cos_hour_day: Double,
        sin_minute_hour: Double,
        cos_minute_hour: Double,
        sin_minute_day: Double,
        cos_minute_day: Double,

        Latitude: Double,
        Longitude: Double,

        TDID: String,
        BidRequestId: String,
        AdvertiserId: Option[Int],
        CampaignId: String,
        AdGroupId: String,
        SyntheticIds: Int, // array type in training
        Targets: Double, // array type in training

        // new features compare to trm model input
        OnlineRelevanceScore: Double,
        DeviceTypeName: String,
        PersonGraphTargets: Double, // array type in training
        HouseholdGraphTargets: Double, // array type in training
        SiteZipHashed: Long,
        AliasedSupplyPublisherIdCityHashed: Long,
        ZipSiteLevel_Seed: Int // array type in training
)

case class RelevanceOnlineDataset(model: String, tag: String, version: Int = 1) extends LightWritableDataset[RelevanceOnlineRecord](s"${ttdWriteEnv}/audience/${model}/${tag}/v=${version}", ML_PLATFORM_ROOT, audienceResultCoalesce, dateFormat = audienceVersionDateFormat)
