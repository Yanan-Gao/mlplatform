package com.thetradedesk.featurestore.datasets

import com.thetradedesk.featurestore.partCount
import org.apache.spark.sql.{Encoder, Encoders}

import scala.reflect.runtime.universe._


final case class BidFeedbackRecord(
                                         BidRequestId: String,
                                         BidFeedbackId: String,
                                         LogEntryTime: java.sql.Timestamp,
                                         AdvertiserId: String,
                                         CampaignId: String,
                                         TDID: String,
                                         AdGroupId: String,
                                         AdvertiserCostInUSD: Option[BigDecimal],
                                         SubmittedBidAmountInUSD: Option[BigDecimal]
                                       )

case class ClickBidFeedbackRecord(
                                         BidRequestId: String,
                                         BidFeedbackId: String,
                                         LogEntryTime: java.sql.Timestamp,
                                         AdvertiserId: String,
                                         CampaignId: String,
                                         AdGroupId: String,
                                         TDID: String,
                                         AdvertiserCostInUSD: Option[BigDecimal],
                                         SubmittedBidAmountInUSD: Option[BigDecimal],
                                         ClickRedirectId: String,
                                       )

case class DailyClickBidFeedbackDataset() extends ProcessedDataset[ClickBidFeedbackRecord] {
  override val defaultNumPartitions: Int = partCount.DailyClickBidFeedback
  override val datasetName: String = "dailyclickbidfeedback"
  override val repartitionColumn: Option[String] = Some("BidRequestId")

  val enc: Encoder[ClickBidFeedbackRecord] = Encoders.product[ClickBidFeedbackRecord]
  val tt: TypeTag[ClickBidFeedbackRecord] = typeTag[ClickBidFeedbackRecord]
}