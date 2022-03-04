package com.thetradedesk.bidsimpression.schema

  object BidCols {
    val BIDSCOLUMNS: Seq[String] = Seq(
      "BidRequestId",
      "DealId",

      "AdjustedBidCPMInUSD",
      "FirstPriceAdjustment",
      "FloorPriceInUSD",

      "PartnerId",
      "AdvertiserId",
      "CampaignId",
      "AdGroupId",

      "SupplyVendor",
      "SupplyVendorPublisherId",
      "SupplyVendorSiteId",
      "Site",
      "ImpressionPlacementId",
      "AdWidthInPixels",
      "AdHeightInPixels",

      "MatchedCategory",
      "MatchedFoldPosition",
      "RenderingContext",
      "ReferrerCategories",

      "VolumeControlPriority",
      "LogEntryTime",


      "AdsTxtSellerType",
      "PublisherType",
      "AuctionType",


      "Country",
      "Region",
      "Metro",
      "City",
      "Zip",


      "DeviceType",
      "DeviceMake",
      "DeviceModel",
      "OperatingSystemFamily",
      "Browser",

      "UserHourOfWeek",
      "RequestLanguages",
      "MatchedLanguageCode",
      "Latitude",
      "Longitude",

      "PredictiveClearingMode",
      "PredictiveClearingRandomControl",

      "CreativeId",
      "DoNotTrack",
      "PrivateContractId"
    )
  }

