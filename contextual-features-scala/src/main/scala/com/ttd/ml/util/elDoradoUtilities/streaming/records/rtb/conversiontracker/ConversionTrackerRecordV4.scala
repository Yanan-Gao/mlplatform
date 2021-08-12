/** MACHINE-GENERATED FROM AVRO SCHEMA. DO NOT EDIT DIRECTLY */
package com.ttd.ml.util.elDoradoUtilities.streaming.records.rtb.conversiontracker

/**
 * @param GroupId 
 * @param LogEntryTime 
 * @param HandlingDurationInTicks 
 * @param ConversionId 
 * @param IPAddress 
 * @param UserAgent 
 * @param ReferrerUrl 
 * @param PartnerId 
 * @param AccountId 
 * @param AdvertiserId 
 * @param ConversionType 
 * @param CampaignId 
 * @param ResponseFormat 
 * @param MonetaryValue 
 * @param MonetaryValueCurrency 
 * @param ProvisioningDeltaId 
 * @param TDIDCookieStatus com.thetradedesk.streaming.records.rtb.CookieStatusType
 * @param TDID 
 * @param AdSrvrCookieStatus 
 * @param AdSrvrCookieValue 
 * @param TDTrackCookieStatus com.thetradedesk.streaming.records.rtb.CookieStatusType
 * @param TDTrackCookieValue 
 * @param RawUrl 
 * @param Country 
 * @param Region 
 * @param Metro 
 * @param City 
 * @param AdvertisingPartnerUserIds DEPRECATED Sept 2013
 * @param Zip 
 * @param OpenStatId 
 * @param OpenStatIdVersion 
 * @param VendorAttributedImpressionId 
 * @param IsVendorAttributedViewThroughConversion 
 * @param UserDataHasBeenProcessed 
 * @param TD1 
 * @param TD2 
 * @param TD3 
 * @param TD4 
 * @param TD5 
 * @param TD6 
 * @param TD7 
 * @param TD8 
 * @param TD9 
 * @param TD10 
 * @param OfflineConversionTime 
 * @param OfflineProviderId 
 * @param AllowTargeting 
 * @param IsGdprApplicable 
 * @param GdprConsent 
 * @param TTDHasConsentForDataSegmenting 
 * @param PartnerHasConsent 
 * @param OrderId
 */
case class ConversionTrackerRecordV4(GroupId: String = "", LogEntryTime: java.sql.Timestamp, HandlingDurationInTicks: Long, ConversionId: String, IPAddress: Option[String] = None, UserAgent: Option[String] = None, ReferrerUrl: Option[String] = None, PartnerId: Option[String] = None, AccountId: Option[String] = None, AdvertiserId: Option[String] = None, ConversionType: Option[String] = None, CampaignId: Option[String] = None, ResponseFormat: Option[String] = None, MonetaryValue: Option[BigDecimal], MonetaryValueCurrency: Option[String] = None, ProvisioningDeltaId: Option[String] = None, TDIDCookieStatus: Int, TDID: String, AdSrvrCookieStatus: Int, AdSrvrCookieValue: Option[String] = None, TDTrackCookieStatus: Int, TDTrackCookieValue: Option[String] = None, RawUrl: Option[String] = None, Country: String, Region: Option[String] = None, Metro: Option[String] = None, City: String, AdvertisingPartnerUserIds: Option[String] = None, Zip: Option[String] = None, OpenStatId: Option[String] = None, OpenStatIdVersion: Option[String] = None, VendorAttributedImpressionId: Option[String], IsVendorAttributedViewThroughConversion: Option[Boolean], UserDataHasBeenProcessed: Boolean, TD1: Option[String] = None, TD2: Option[String] = None, TD3: Option[String] = None, TD4: Option[String] = None, TD5: Option[String] = None, TD6: Option[String] = None, TD7: Option[String] = None, TD8: Option[String] = None, TD9: Option[String] = None, TD10: Option[String] = None, OfflineConversionTime: Option[java.sql.Timestamp], OfflineProviderId: Option[String] = None, AllowTargeting: Boolean, IsGdprApplicable: Boolean, GdprConsent: Option[String] = None, TTDHasConsentForDataSegmenting: Boolean, PartnerHasConsent: Boolean, OrderId: Option[String] = None)