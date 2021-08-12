package com.ttd.ml.util.elDoradoUtilities.streaming.records.rtb

/**
 * Analog of C# class RTBAgileGrainKey
 * @param ReportDateTimeUtc Needs to be in the key for the SQL Connector? Used as the window start time for windowed stream operators
 * @param CreativeId
 * @param AdGroupId
 * @param CampaignId
 * @param AdvertiserId
 * @param PartnerId
 * @param PrivateContractId
 * @param MediaTypeId
 * @param RenderingContextId
 * @param DeviceTypeId
 * @param CampaignFlightId
 * @param LateDataProviderId TODO(jonathan.miles): find the source of this. Is it ever set or always default value?
 */
case class AgilesGrainRecord(ReportDateTimeUtc: Option[java.sql.Timestamp] = None, CreativeId: String, AdGroupId: String, CampaignId: String, AdvertiserId: String, PartnerId: String, PrivateContractId: String = "[novalue]", MediaTypeId: Int, RenderingContextId: Int = 1, DeviceTypeId: Int = 0, CampaignFlightId: Long = -1L, LateDataProviderId: Int = -1)
