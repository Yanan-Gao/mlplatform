package com.thetradedesk.kongming.transform

import com.thetradedesk.geronimo.shared.{ARRAY_INT_FEATURE_TYPE, FLOAT_FEATURE_TYPE, GERONIMO_DATA_SOURCE, INT_FEATURE_TYPE, STRING_FEATURE_TYPE, loadParquetData, shiftModUdf}
import com.thetradedesk.kongming.datasets._
import com.thetradedesk.kongming.date
import org.apache.spark.sql.Dataset
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._
import com.thetradedesk.spark.sql.SQLFunctions._
import com.thetradedesk.spark.TTDSparkContext.spark.implicits._

import java.time.LocalDate

object AudienceIdTransform {
  val SHAPE_AUDIENCEID_CATEGORY: Int = 30
  val CARDINALITY_AUDIENCEID_CATEGORY: Int =5002
  final case class AudienceFeature(
                                    AdvertiserId: String,
                                    CampaignId: String
                                  )

  final case class AudienceList(
                                 AudienceId: Array[Int],
                                 CampaignId: String,
                                 AdvertiserId: String
                               )

  // consider joining trainset here first or later. might need to consider whether it is efficient to join all campaign data.
  def generateAudiencelist (
                             uniqueCampaign: Dataset[AudienceFeature],
                             date: LocalDate
                           ) : Dataset[AudienceList] = {

    val adgroupTable = UnifiedAdGroupFeatureDataSet().readLatestPartitionUpTo(date, isInclusive = true).select("CampaignId", "AudienceId")
    val modifiedDataset = broadcast(uniqueCampaign).join(adgroupTable, Seq("CampaignId"), "left")
    val hashedTrain = modifiedDataset
      .withColumn("AudienceId", when(col("AudienceId").isNotNull, shiftModUdf(xxhash64(col("AudienceId")), lit(CARDINALITY_AUDIENCEID_CATEGORY))).otherwise(0))
    val audienceList = hashedTrain.groupBy("CampaignId","AdvertiserId").agg(collect_set("AudienceId").alias("AudienceId"))
      .withColumn("AudienceId", slice(array_sort($"AudienceId"), 1, SHAPE_AUDIENCEID_CATEGORY))
      .selectAs[AudienceList]

    audienceList

  }

  def generateAudienceTable(
                            date: LocalDate
                          ): Dataset[AudienceList] = {

    val adgroupTable = UnifiedAdGroupFeatureDataSet().readLatestPartitionUpTo(date, isInclusive = true).select("CampaignId", "AudienceId","AdvertiserId")
    val hashedAudienceId = adgroupTable
      .withColumn("AudienceId", when(col("AudienceId").isNotNull, shiftModUdf(xxhash64(col("AudienceId")), lit(CARDINALITY_AUDIENCEID_CATEGORY))).otherwise(0))
    val audienceList = hashedAudienceId
      .groupBy("CampaignId", "AdvertiserId")
      .agg(collect_set("AudienceId").alias("AudienceId"))
      .withColumn("AudienceId", expr(s"concat(slice(array_sort(AudienceId), 1, $SHAPE_AUDIENCEID_CATEGORY), array_repeat(0, $SHAPE_AUDIENCEID_CATEGORY - size(AudienceId)))"))
      .selectAs[AudienceList]

    audienceList

  }
}

