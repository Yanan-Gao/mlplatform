/** MACHINE-GENERATED FROM AVRO SCHEMA. DO NOT EDIT DIRECTLY */
package com.ttd.ml.util.elDoradoUtilities.streaming.records.rtb.attributedevent

import com.ttd.ml.util.elDoradoUtilities.streaming.records.rtb.{DeviceTypeLookupRecord, RenderingContextLookupRecord, Uuid}

/**
 * This file contains a subset of the AttributedEventResult Vertica table file
 * @param GroupId 
 * @param LogFileId 
 * @param ConversionTrackerLogFileId 
 * @param ConversionTrackerLogEntryTime 
 * @param ConversionTrackerId 
 * @param AttributedEventLogFileId 
 * @param AttributedEventLogEntryTime 
 * @param AttributedEventTypeId 
 * @param AttributedEventIntId1 
 * @param AttributedEventIntId2 
 * @param CreativeId 
 * @param AdGroupId 
 * @param CampaignId 
 * @param AdvertiserId 
 * @param PartnerId 
 * @param DealId 
 * @param PrivateContractId 
 * @param RenderingContext 
 * @param AdFormat 
 * @param SupplyVendor 
 * @param DeviceType 
 * @param CampaignFlightId 
 * @param Country 
 * @param Region 
 * @param Metro 
 * @param City 
 * @param Site 
 * @param RecencyBucketStartInMinutes RecencyBucketStart. In minutes. The lower bound of the recency bucket this recency falls into.
 * @param RecencyBucketEndInMinutes RecencyBucketEnd. In minutes. The upper bound of the recency bucket this recency falls into.
 * @param MatchedCategory 
 * @param CustomCPACount 
 * @param AttributionMethodId 
 * @param ReportingColumnId 
 * @param AttributedCount 
 * @param AttributedRevenue 
 * @param ConversionMultiplier 
 * @param PrivateContractOwningPartnerId 
 * @param PrivateContractOwningPartnerCurrencyCodeId 
 */
case class AttributedEventResultRecord(GroupId: String = "", LogFileId: Long, ConversionTrackerLogFileId: Long, ConversionTrackerLogEntryTime: java.sql.Timestamp, ConversionTrackerId: Uuid, AttributedEventLogFileId: Long, AttributedEventLogEntryTime: java.sql.Timestamp, AttributedEventTypeId: Int, AttributedEventIntId1: Long, AttributedEventIntId2: Long, CreativeId: Option[String], AdGroupId: Option[String], CampaignId: Option[String], AdvertiserId: Option[String], PartnerId: Option[String], DealId: Option[String] = None, PrivateContractId: Option[String] = None, RenderingContext: Option[RenderingContextLookupRecord], AdFormat: Option[String], SupplyVendor: Option[String] = None, DeviceType: Option[DeviceTypeLookupRecord], CampaignFlightId: Option[Long] = None, Country: Option[String] = None, Region: Option[String] = None, Metro: Option[String] = None, City: Option[String] = None, Site: Option[String] = None, RecencyBucketStartInMinutes: Option[Int] = None, RecencyBucketEndInMinutes: Option[Int] = None, MatchedCategory: Option[String] = None, CustomCPACount: Option[BigDecimal] = None, AttributionMethodId: Int, ReportingColumnId: Int, AttributedCount: BigDecimal, AttributedRevenue: BigDecimal, ConversionMultiplier: Int, PrivateContractOwningPartnerId: Option[String] = None, PrivateContractOwningPartnerCurrencyCodeId: Option[String] = None)