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
        // ContextualCategoriesTier1: Array[Int],
        MatchedSegments: Array[Long],
        MatchedSegmentsLength: Float,
        HasMatchedSegments: Option[Int],
        UserSegmentCount: Float,
        AdWidthInPixels: Float,
        AdHeightInPixels: Float,

        sin_hour_week: Float,
        cos_hour_week: Float,
        sin_hour_day: Float,
        cos_hour_day: Float,
        sin_minute_hour: Float,
        cos_minute_hour: Float,
        sin_minute_day: Float,
        cos_minute_day: Float,

        Latitude: Float,
        Longitude: Float,

        TDID: String,
        DeviceAdvertisingId: String,
        CookieTDID: String,
        UnifiedId2: String,
        EUID: String,
        IdentityLinkId: String,
        BidRequestId: String,
        AdvertiserId: Option[Int],
        CampaignId: String,
        AdGroupId: String,
        SyntheticIds: Seq[Int], // array type in training
        Targets: Seq[Float], // array type in training
        SampleWeights: Seq[Float], // sample weight used during training

        // new features compare to trm model input
        OnlineRelevanceScore: Float,
        DeviceTypeName: String,
        PersonGraphTargets: Seq[Float], // array type in training
        HouseholdGraphTargets: Seq[Float], // array type in training
        SiteZipHashed: Long,
        AliasedSupplyPublisherIdCityHashed: Long,
        ZipSiteLevel_Seed: Seq[Int], // array type in training
        IdTypesBitmap: Int,
        BidRequestIdmostSigBits: Long, // cbuffer can only read non string value
        BidRequestIdleastSigBits: Long, // cbuffer can only read non string value
        TDIDmostSigBits: Long, // cbuffer can only read non string value
        TDIDleastSigBits: Long // cbuffer can only read non string value
)

case class RelevanceOnlineDataset(model: String, tag: String, version: Int = 1) extends LightWritableDataset[RelevanceOnlineRecord](s"${ttdWriteEnv}/audience/${model}/${tag}/v=${version}", ML_PLATFORM_ROOT, audienceResultCoalesce, dateFormat = audienceVersionDateFormat)
