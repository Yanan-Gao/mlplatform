package com.thetradedesk.plutus.data.schema

case class PlutusRawDataset(
                             BidRequestId: String,
                             DealId: String,

                             PartnerId: String,
                             AdvertiserId: String,
                             CampaignId: String,
                             AdGroupId: String,
                             SupplyVendor: String,
                             SupplyVendorPublisherId: String,

                             AliasedSupplyPublisherId: Integer,
                             AspSvpId: String,

                             SupplyVendorSiteId: String,
                             Site: String,

                             ImpressionPlacementId: String,

                             MatchedCategoryList: List[String],
                             MatchedFoldPosition: Integer,
                             RenderingContext: Integer,

                             AdFormat: String,

                             VolumeControlPriority: Integer,

                             LogEntryTime: java.sql.Timestamp,

                             sin_hour_day: Double,
                             cos_hour_day: Double,
                             sin_hour_week: Double,
                             cos_hour_week: Double,
                             sin_minute_hour: Double,
                             cos_minute_hour: Double,
                             sin_minute_day: Double,
                             cos_minute_day: Double,
                             UserHourOfWeek: Integer,

                             AdsTxtSellerType: Integer,
                             PublisherType: Integer,

                             Country: String,
                             Region: String,
                             Metro: String,
                             City: String,
                             Zip: String,

                             DeviceType: Integer,
                             DeviceMake: String,
                             DeviceModel: String,
                             OperatingSystemFamily: Integer,
                             Browser: Integer,
                             RequestLanguages: String,

                             Latitude: Double,
                             Longitude: Double,
                             PredictiveClearingMode: Integer,
                             PredictiveClearingRandomControl: Boolean,

                             RealMediaCost: Double,
                             AdjustedBidCPMInUSD: Double,


                             IsImp: Boolean,
                             mbtw: Double,
                             AuctionBidPrice: Double,
                             FloorPriceInUSD: Double,
                           )
