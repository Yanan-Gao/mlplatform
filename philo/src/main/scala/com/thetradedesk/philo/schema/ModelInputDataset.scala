package com.thetradedesk.philo.schema

case class ModelInputRecord(
    BidRequestId: String,
    AdFormat: String,
    AdGroupId: String,
    AdvertiserId: String,
    PartnerId: String,
    AudienceId: String,
    IndustryCategoryId: String,
    AdsTxtSellerType: String,
    PublisherType: String,
    Metro: String,
    Zip: String,
    City: String,
    Country: String,
    Region: String,
    Browser: String,
    DeviceMake: String,
    DeviceModel: String,
    DeviceType: String,
    CampaignId: String,
    CreativeId: String,
    //CreativeLandingPageId: String,
    DoNotTrack: Int,
    ImpressionPlacementId: String,
    OperatingSystemFamily: String,
    RenderingContext: String,
    RequestLanguages: String,
    Site: String,
    SupplyVendor: String,
    SupplyVendorPublisherId: String,
    AliasedSupplyPublisherId: Int,
    SupplyVendorSiteId: String,
    MatchedFoldPosition: Int,
    AdWidthInPixels: String,
    AdHeightInPixels: String,
    UserHourOfWeek: Int,
    sin_hour_day: Double,
    cos_hour_day: Double,
    sin_hour_week: Double,
    cos_hour_week: Double,
    sin_minute_hour: Double,
    cos_minute_hour: Double,
    PrivateContractId: String,
    latitude: Double,
    longitude: Double,
    label: Int,
    OriginalAdGroupId: String,
    OriginalCountry: String,

    UIID: String,
    LogEntryTime: java.sql.Timestamp,
    excluded: Int

)


object ModelInputDataSet {
  val PHILOS3 = (env: String) => f"s3://thetradedesk-mlplatform-us-east-1/features/data/philo/v=1/${env}/processed/"
  val FILTERED = (env: String) => f"s3://thetradedesk-mlplatform-us-east-1/features/data/philo/v=1/${env}/filtered/"
  val APAC = (env: String) =>  f"s3://thetradedesk-mlplatform-us-east-1/features/data/philo/v=1/${env}/apac/"
}
