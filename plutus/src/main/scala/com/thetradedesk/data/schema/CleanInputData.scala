package com.thetradedesk.data.schema

final case class CleanInputData(supplyVendor: String, // ImpressionContext.BiddingRequest.SupplyVendorName
                                dealId: String, //Bid.MatchedPrivateContract?.SupplyVendorDealCode
                                supplyVendorPublisherId: String, //ImpressionContext.BiddingRequest.SupplyVendorPublisherId
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

                                is_imp: Double,
                                AuctionBidPrice: Double,
                                RealMediaCost: Double,
                                mb2w: Double,
                                FloorPriceInUSD: Double
                               )
