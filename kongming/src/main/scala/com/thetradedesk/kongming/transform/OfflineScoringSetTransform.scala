package com.thetradedesk.kongming.transform

import com.thetradedesk.spark.sql.SQLFunctions._
import com.thetradedesk.kongming.datasets.{AdGroupPolicyRecord, BidsImpressionsSchema, DailyOfflineScoringRecord, UnifiedAdGroupFeatureDataSet,AdvertiserFeatureDataSet}
import com.thetradedesk.kongming.transform.ContextualTransform.ContextualData
import com.thetradedesk.spark.TTDSparkContext.spark.implicits._
import com.thetradedesk.spark.util.prometheus.PrometheusClient
import com.thetradedesk.kongming.transform.AudienceIdTransform.AudienceFeature
import org.apache.spark.sql.{Column, Dataset}
import org.apache.spark.sql.functions.{col, concat, lit, substring, when}

import java.time.LocalDate

object OfflineScoringSetTransform {
  val DEFAULT_NUM_PARTITION = 200

  /**
   * Getting policy adgroup's impression data for scoring purpose.
   * @param bidsImpressions bids impression dataset
   * @param adGroupPolicy policy dataset
   * @param selectionTabular column transformations
   * @param prometheus
   * @return
   */
  def dailyTransform(date: LocalDate,
                     bidsImpressions: Dataset[BidsImpressionsSchema],
                     selectionTabular: Array[Column]
                    )
                    (implicit prometheus: PrometheusClient): Dataset[DailyOfflineScoringRecord] = {
    val bidsImp = bidsImpressions.filter('IsImp)
      //Assuming ConfigKey will always be adgroupId.
      .withColumn("AdFormat",concat(col("AdWidthInPixels"),lit('x'), col("AdHeightInPixels")))
      .withColumn("RenderingContext", $"RenderingContext.value")
      .withColumn("DeviceType", $"DeviceType.value")
      .withColumn("OperatingSystem", $"OperatingSystem.value")
      .withColumn("Browser", $"Browser.value")
      .withColumn("InternetConnectionType", $"InternetConnectionType.value")
      .withColumn("IsTracked", when($"UIID".isNotNullOrEmpty && $"UIID" =!= lit("00000000-0000-0000-0000-000000000000"), lit(1)).otherwise(0))
      .withColumn("IsUID2", when(substring($"UIID", 9, 1) =!= lit("-"), lit(1)).otherwise(0))

    val bidsImpContextual = ContextualTransform.generateContextualFeatureTier1(
      bidsImp.select("BidRequestId","ContextualCategories")
        .dropDuplicates("BidRequestId").selectAs[ContextualData]
    )

    val dimAudienceId = AudienceIdTransform.generateAudiencelist(bidsImp.selectAs[AudienceFeature],date).cache()
    val dimIndustryCategoryId = AdvertiserFeatureDataSet().readLatestPartitionUpTo(date, isInclusive = true).select("AdvertiserId","IndustryCategoryId")
      .withColumn("IndustryCategoryId", col("IndustryCategoryId").cast("Int")).cache()

    bidsImp
      .join(bidsImpContextual, Seq("BidRequestId"), "left")
      .join(dimAudienceId, Seq("CampaignId", "AdvertiserId"), "left")
      .join(dimIndustryCategoryId, Seq("AdvertiserId"), "left")
      .select(selectionTabular: _*)
      .as[DailyOfflineScoringRecord]
  }
}
