package com.thetradedesk.featurestore.datasets

import com.thetradedesk.featurestore.partCount
import org.apache.spark.sql.{Encoder, Encoders}

import scala.reflect.runtime.universe._
case class DailyAttributionRecord(
                                         AttributedEventId: String,
                                         AttributedEventTypeId: String,
                                         ConversionTrackerLogFileId: String,
                                         ConversionTrackerIntId1: String,
                                         ConversionTrackerIntId2: String,
                                         AttributedEventLogFileId: String,
                                         AttributedEventIntId1: String,
                                         AttributedEventIntId2: String,
                                         AttributedEventLogEntryTime: String,// is string in parquet
                                         ConversionTrackerId: String,
                                         TrackingTagId: String,
                                         TDID: String,
                                         AdvertiserId: String,
                                         CampaignId: String,
                                         AdGroupId: String,
                                         MonetaryValue: Option[String],
                                         MonetaryValueCurrency: Option[String],
                                         ConversionTrackerLogEntryTime: String,  // is string in parquet.
                                         CampaignReportingColumnId: String,
                                         AttributionMethodId: String,
                                         Revenue: String,
                                       )


case class DailyAttributionDataset() extends ProcessedDataset[DailyAttributionRecord] {
  override val defaultNumPartitions: Int = partCount.DailyAttribution
  override val datasetName: String = "dailyattribution"

  val enc: Encoder[DailyAttributionRecord] = Encoders.product[DailyAttributionRecord]
  val tt: TypeTag[DailyAttributionRecord] = typeTag[DailyAttributionRecord]
}
