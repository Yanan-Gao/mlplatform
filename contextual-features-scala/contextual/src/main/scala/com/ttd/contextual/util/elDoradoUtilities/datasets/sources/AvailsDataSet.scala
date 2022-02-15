package com.ttd.contextual.util.elDoradoUtilities.datasets.sources

import com.ttd.contextual.util.elDoradoUtilities.datasets.core.{ColumnExistsInDataSet, DateHourPartitionedS3DataSet, S3Roots, SourceDataSet}

case class Avails7DayDataSet() extends DateHourPartitionedS3DataSet[AvailLog](
  SourceDataSet,
  S3Roots.IDENTITY_ROOT,
  "avails7day/v=2",
  dateField = "date" -> ColumnExistsInDataSet,
  hourField = "hour" -> ColumnExistsInDataSet)

case class Avails30DayDataSet() extends DateHourPartitionedS3DataSet[AvailLog](
  SourceDataSet,
  S3Roots.IDENTITY_ROOT,
  "avails30day/v=2",
  dateField = "date" -> ColumnExistsInDataSet,
  hourField = "hour" -> ColumnExistsInDataSet)

case class AvailLog(
                     TimeStamp: Long,
                     TdidGuid: Option[String],
                     DeviceIdGuid: Option[String],
                     IpAddressBytes: Seq[Byte],
                     IpAddressMasked: Boolean,
                     SupplyVendorId: Int,
                     UserAgent: String,
                     Urls: Seq[String],
                     DoNotTrack: Boolean,
                     LimitAdTracking: Boolean,
                     Country: String,
                     IsGdprRegulated: Boolean,
                     GdprConsentToken: String,
                     Categories: Seq[String],
                     Latitude: Int,
                     Longitude: Int,
                     DeviceMake: String,
                     DeviceModel: String,
                     DeviceType: Int,
                     RenderingContext: Int,
                     DealIds: Seq[String],
                     SupplyVendorPublisherId: String,
                     Region: String,
                     OptOutReason: Int,
                     CreativeAdFormatTypes: Seq[Int],
                     ImpressionPlacementIds: Seq[String],
                     GdprAllowedPartnersMask: Int,
                     TargetingIdType: Int,
                     DealMetadataByTag: Seq[DealMetadataByTag],
                     IsInUnsampledSet: Boolean,
                     IsInInternalSampledSet: Boolean)

case class DealMetadataByTag(Tag: String, DealIdIndexes: Seq[Int])
