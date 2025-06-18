package com.thetradedesk.featurestore.datasets

import org.apache.spark.sql.{Encoder, Encoders}

import scala.reflect.runtime.universe
import scala.reflect.runtime.universe._


final case class BidContextualRecord( // bidrequest cols
                                      BidRequestId: String,
                                      UIID: Option[String],
                                      AdvertiserId: Option[String],
                                      CampaignId: Option[String],
                                      ContextualEmbeddingCategories: Seq[Int],
                                      ContextualProbabilityScores: Seq[Double]
                                    )

case class BidContextualRecordDataset(root: String, prefix: String) extends LightReadableDataset[BidContextualRecord] {

  val enc: Encoder[BidContextualRecord] = Encoders.product[BidContextualRecord]
  val tt: universe.TypeTag[BidContextualRecord] = typeTag[BidContextualRecord]

  override val dataSetPath: String = prefix
  override val rootPath: String = root
}