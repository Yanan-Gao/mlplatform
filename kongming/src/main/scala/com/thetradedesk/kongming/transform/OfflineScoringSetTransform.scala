package com.thetradedesk.kongming.transform

import com.thetradedesk.geronimo.shared.encodeStringIdUdf
import com.thetradedesk.spark.sql.SQLFunctions._
import com.thetradedesk.kongming.datasets._
import com.thetradedesk.kongming.transform.ContextualTransform.ContextualData
import com.thetradedesk.spark.TTDSparkContext.spark.implicits._
import com.thetradedesk.spark.util.prometheus.PrometheusClient
import com.thetradedesk.kongming.transform.AudienceIdTransform.AudienceFeature
import org.apache.spark.sql.{Column, Dataset}
import org.apache.spark.sql.functions.{col, concat, lit, substring, when, date_format, size}

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
                    (implicit prometheus: PrometheusClient): Dataset[UnionDailyOfflineScoringRecord] = {
    val bidsImp = bidsImpressions.filter('IsImp)
      //Assuming ConfigKey will always be adgroupId.
      .withColumn("LogEntryTime", date_format($"LogEntryTime", "yyyy-MM-dd HH:mm:ss"))
      .withColumn("AdFormat",concat(col("AdWidthInPixels"),lit('x'), col("AdHeightInPixels")))
      .withColumn("RenderingContext", $"RenderingContext.value")
      .withColumn("DeviceType", $"DeviceType.value")
      .withColumn("OperatingSystem", $"OperatingSystem.value")
      .withColumn("Browser", $"Browser.value")
      .withColumn("InternetConnectionType", $"InternetConnectionType.value")
      .withColumn("IsTracked", when($"UIID".isNotNullOrEmpty && $"UIID" =!= lit("00000000-0000-0000-0000-000000000000"), lit(1)).otherwise(0))
      .withColumn("HasUserData", when($"MatchedSegments".isNull||size($"MatchedSegments")===lit(0), lit(0)).otherwise(lit(1)))
      .withColumn("UserDataLength", when($"UserSegmentCount".isNull, lit(0.0)).otherwise($"UserSegmentCount"*lit(1.0)))
      .withColumn("UserData", when($"HasUserData"===lit(0), lit(null)).otherwise($"MatchedSegments"))
      .cache()

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
      .withColumn("AdGroupIdEncoded", encodeStringIdUdf('AdGroupId))
      .withColumn("CampaignIdEncoded", encodeStringIdUdf('CampaignId))
      .withColumn("AdvertiserIdEncoded", encodeStringIdUdf('AdvertiserId))
      .withColumn("sin_hour_week", $"sin_hour_week".cast("float"))
      .withColumn("cos_hour_week", $"cos_hour_week".cast("float"))
      .withColumn("sin_hour_day", $"sin_hour_day".cast("float"))
      .withColumn("cos_hour_day", $"cos_hour_day".cast("float"))
      .withColumn("sin_minute_hour", $"sin_minute_hour".cast("float"))
      .withColumn("cos_minute_hour", $"cos_minute_hour".cast("float"))
      .withColumn("latitude", $"latitude".cast("float"))
      .withColumn("longitude", $"longitude".cast("float"))
      .withColumn("UserDataLength", $"UserDataLength".cast("float"))
      .withColumn("ContextualCategoryLengthTier1", $"ContextualCategoryLengthTier1".cast("float"))
      .select(selectionTabular: _*)
      .selectAs[UnionDailyOfflineScoringRecord]
  }
}
