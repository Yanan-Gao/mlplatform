package com.thetradedesk.pythia.interestModel.schema

// ---------------------------------------
// DATA SCHEMA FOR INTEREST MODEL (= Output of ETL pipeline)
// ---------------------------------------
case class ModelInputRecord(
    Labels: Seq[Int],
    RenderingContext: Int,
    DeviceType: Int,
    OperatingSystem: Int,
    OperatingSystemFamily: Int,
    Browser: Int,    
    InternetConnectionType: Int,
    PublisherType: Int,
    SupplyVendor: String,
    SupplyVendorPublisherId: String,
    SupplyVendorSiteId: String,
    Site: String,
    ReferrerUrl: String,
    Country: String,
    Region: String,
    Metro: String,
    City: String,
    Zip: String,
    DeviceMake: String,
    DeviceModel: String,
    RequestLanguages: String,
    MatchedLanguageCode: String,
    Latitude: Double,
    Longitude: Double,
    UserHourOfWeek: Int,
    sin_hour_week: Double,
    cos_hour_week: Double,
    sin_hour_day: Double,
    cos_hour_day: Double,
    sin_minute_hour: Double,
    cos_minute_hour: Double,
    sin_minute_day: Double,
    cos_minute_day: Double
    // ContextualCategories: Seq[Long],
)

object ModelInputDataset {
  val PYTHIAINTERESTSS3 = (env: String) => f"s3://thetradedesk-mlplatform-us-east-1/features/data/pythia/interests/v=1/${env}/"
}