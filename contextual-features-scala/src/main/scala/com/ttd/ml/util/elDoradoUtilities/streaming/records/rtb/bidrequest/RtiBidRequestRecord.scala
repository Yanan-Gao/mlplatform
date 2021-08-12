/** MACHINE-GENERATED FROM AVRO SCHEMA. DO NOT EDIT DIRECTLY */
package com.ttd.ml.util.elDoradoUtilities.streaming.records.rtb.bidrequest

import com.ttd.ml.util.elDoradoUtilities.streaming.records.rtb.{DataUsageRecord, DeviceTypeLookupRecord, RenderingContextLookupRecord, Uuid}

/**
 * Cut down version of BidRequestRecord used to minimally deserialize records RTI's specific use
 * @param GroupId 
 * @param LogEntryTime 
 * @param BidRequestId 
 * @param CreativeId RTI Key Value
 * @param AdGroupId RTI Key Value
 * @param CampaignId RTI Key Value
 * @param AdvertiserId RTI Key Value
 * @param PartnerId RTI Key Value
 * @param PrivateContractId RTI Key Value
 * @param DealId RTI Key Value
 * @param AdWidthInPixels 
 * @param AdHeightInPixels 
 * @param RenderingContext 
 * @param DeviceType 
 * @param CampaignFlightId 
 * @param Country 
 * @param Region 
 * @param Metro 
 * @param City 
 * @param Site 
 * @param MatchedCategory 
 * @param SupplyVendor 
 * @param SupplyVendorPublisherId 
 * @param DataUsageRecord 
 * @param AdjustedBidCPMInUSD RTI Value Field
 * @param AdvertiserCurrencyExchangeRateFromUSD RTI Value Field
 * @param PartnerCurrencyExchangeRateFromUSD RTI Value Field
 * @param PrivateContractOwningPartnerId 
 * @param PrivateContractOwningPartnerCurrencyCodeId 
 * @param PrivateContractOwningPartnerCurrencyExchangeRate 
 */
case class RtiBidRequestRecord(GroupId: String = "", LogEntryTime: java.sql.Timestamp, BidRequestId: Uuid, CreativeId: Option[String], AdGroupId: Option[String], CampaignId: Option[String], AdvertiserId: Option[String], PartnerId: Option[String], PrivateContractId: Option[String], DealId: Option[String], AdWidthInPixels: Option[Int], AdHeightInPixels: Option[Int], RenderingContext: Option[RenderingContextLookupRecord], DeviceType: Option[DeviceTypeLookupRecord], CampaignFlightId: Option[Long], Country: Option[String], Region: Option[String], Metro: Option[String], City: Option[String], Site: Option[String], MatchedCategory: Option[String], SupplyVendor: Option[String], SupplyVendorPublisherId: Option[String], DataUsageRecord: DataUsageRecord, AdjustedBidCPMInUSD: BigDecimal, AdvertiserCurrencyExchangeRateFromUSD: BigDecimal, PartnerCurrencyExchangeRateFromUSD: BigDecimal, PrivateContractOwningPartnerId: Option[String] = None, PrivateContractOwningPartnerCurrencyCodeId: Option[String] = None, PrivateContractOwningPartnerCurrencyExchangeRate: Option[BigDecimal] = None)