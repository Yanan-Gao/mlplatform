package com.thetradedesk.plutus.data.schema

import com.thetradedesk.geronimo.shared.{FLOAT_FEATURE_TYPE, INT_FEATURE_TYPE, STRING_FEATURE_TYPE}
import com.thetradedesk.geronimo.shared.schemas.ModelFeature

final case class PlutusTrainingDataset(
                                        supplyVendor: String, // ImpressionContext.BiddingRequest.SupplyVendorName
                                        dealId: String, //Bid.MatchedPrivateContract?.SupplyVendorDealCode
                                        supplyVendorPublisherId: String, //ImpressionContext.BiddingRequest.SupplyVendorPublisherId
                                        aspSvpId: String,
                                        supplyVendorSiteId: String, //ImpressionContext.BiddingRequest.SupplyVendorSiteId
                                        site: String, //ImpressionContext.Site
                                        adFormat: String, //Bid.MatchedAdFormat
                                        //   matchedCategory: String, not in impression context, et. al.
                                        impressionPlacementId: String, // AdInfo.impressionPlacementId
                                        // carrier: String,
                                        country: String, //ImpressionContext.BiddingRequest.GeoLocation.CountryShort
                                        region: String, //ImpressionContext.BiddingRequest.GeoLocation.Region
                                        metro: String, //ImpressionContext.BiddingRequest.GeoLocation.Metro
                                        city: String, //ImpressionContext.BiddingRequest.GeoLocation.City
                                        zip: String, //ImpressionContext.BiddingRequest.GeoLocation.Zip
                                        deviceMake: String, //ImpressionContext.BiddingRequest.DeviceMake
                                        deviceModel: String, //ImpressionContext.BiddingRequest.DeviceModel
                                        requestLanguages: String, //ImpressionContext.BiddingRequest.Languages

                                        renderingContext: Int, //ImpressionContext.BiddingRequest.RenderingContext
                                        aliasedSupplyPublisherId: Int,
                                        // matchedFoldPosition: Int, not in impression context / adinfo
                                        // volumeControlPriority: Int, not in impression context / adinfo
                                        userHourOfWeek: Int, //ImpressionContext.BiddingRequest.UserHourOfWeek
                                        adsTxtSellerType: Int, //ImpressionContext.BiddingRequest.AdsTxtSellerType
                                        publisherType: Int, //ImpressionContext.BiddingRequest.InventoryPublisherType
                                        // internetConnectionType: Int,
                                        deviceType: Int, //impressionContext.BiddingRequest.DeviceType
                                        operatingSystemFamily: Int, //ImpressionContext.BiddingRequest.OSFamily
                                        browser: Int, //ImpressionContext.BiddingRequest.Browser

                                        latitude: Double, //ImpressionContext.BiddingRequest.GeoLocation.LatitudeInDegrees
                                        longitude: Double, //ImpressionContext.BiddingRequest.GeoLocation.LongitudeInDegrees

                                        sin_hour_day: Double,
                                        cos_hour_day: Double,

                                        sin_minute_hour: Double,
                                        cos_minute_hour: Double,

                                        sin_hour_week: Double,
                                        cos_hour_week: Double,

                                        IsImp: Boolean,
                                        AuctionBidPrice: Double,
                                        RealMediaCost: Double,
                                        mbtw: Double,
                                        FloorPriceInUSD: Double
                                      )


object PlutusTrainingDataset {
  val DATA_FEATURES: Array[ModelFeature] = Array(
    ModelFeature("SupplyVendor", STRING_FEATURE_TYPE, Some(102), 0),
    ModelFeature("DealId", STRING_FEATURE_TYPE, Some(5002), 0),
    ModelFeature("SupplyVendorPublisherId", STRING_FEATURE_TYPE, Some(15002), 0),
    ModelFeature("AspSvpId", STRING_FEATURE_TYPE, Some(15002), 0),
    ModelFeature("SupplyVendorSiteId", STRING_FEATURE_TYPE, Some(102), 0),
    ModelFeature("Site", STRING_FEATURE_TYPE, Some(350002), 0),
    ModelFeature("AdFormat", STRING_FEATURE_TYPE, Some(102), 0),
    ModelFeature("ImpressionPlacementId", STRING_FEATURE_TYPE, Some(102), 0),
    ModelFeature("Country", STRING_FEATURE_TYPE, Some(252), 0),
    ModelFeature("Region", STRING_FEATURE_TYPE, Some(4002), 0),
    ModelFeature("Metro", STRING_FEATURE_TYPE, Some(302), 0),
    ModelFeature("City", STRING_FEATURE_TYPE, Some(75002), 0),
    ModelFeature("Zip", STRING_FEATURE_TYPE, Some(90002), 0),
    ModelFeature("DeviceMake", STRING_FEATURE_TYPE, Some(1002), 0),
    ModelFeature("DeviceModel", STRING_FEATURE_TYPE, Some(10002), 0),
    ModelFeature("RequestLanguages", STRING_FEATURE_TYPE, Some(502), 0),

    // these are already integers
    ModelFeature("AliasedSupplyPublisherId", INT_FEATURE_TYPE, Some(15002), 0),
    ModelFeature("RenderingContext", INT_FEATURE_TYPE, Some(6), 0),
    ModelFeature("UserHourOfWeek", INT_FEATURE_TYPE, Some(24 * 7 + 2), 0),
    ModelFeature("AdsTxtSellerType", INT_FEATURE_TYPE, Some(7), 0),
    ModelFeature("PublisherType", INT_FEATURE_TYPE, Some(7), 0),
    ModelFeature("DeviceType", INT_FEATURE_TYPE, Some(9), 0),
    ModelFeature("OperatingSystemFamily", INT_FEATURE_TYPE, Some(10), 0),
    ModelFeature("Browser", INT_FEATURE_TYPE, Some(20), 0),

    ModelFeature("sin_hour_day", FLOAT_FEATURE_TYPE, None, 0),
    ModelFeature("cos_hour_day", FLOAT_FEATURE_TYPE, None, 0),
    ModelFeature("sin_minute_hour", FLOAT_FEATURE_TYPE, None, 0),
    ModelFeature("cos_minute_hour", FLOAT_FEATURE_TYPE, None, 0),
    ModelFeature("sin_hour_week", FLOAT_FEATURE_TYPE, None, 0),
    ModelFeature("cos_hour_week", FLOAT_FEATURE_TYPE, None, 0),
    ModelFeature("latitude", FLOAT_FEATURE_TYPE, None, 0),
    ModelFeature("longitude", FLOAT_FEATURE_TYPE, None, 0)
  )

  val DATA_TARGETS: Array[ModelTarget] = Array(
    ModelTarget("IsImp", "float", nullable = false),
    ModelTarget("AuctionBidPrice", "float", nullable = false),
    ModelTarget("RealMediaCost", "float", nullable = true),
    ModelTarget("mbtw", "float", nullable = false),
    ModelTarget("FloorPriceInUSD", "float", nullable = true),
  )
}