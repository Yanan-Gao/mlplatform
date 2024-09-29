package job

import com.thetradedesk.kongming._
import com.thetradedesk.kongming.datasets._
import com.thetradedesk.spark.TTDSparkContext.spark.implicits._
import com.thetradedesk.spark.datasets.core.DefaultTimeFormatStrings
import com.thetradedesk.spark.sql.SQLFunctions._
import job.AdGroupPolicyGenerator.getActiveAdGroups
import job.GenerateTrainSetRevenueLastTouch.conversionLookback
import org.apache.spark.sql.Dataset
import org.apache.spark.sql.functions._

import java.time.LocalDate

object DailyAttributedEvents extends KongmingBaseJob {

  override def jobName: String = "DailyAttributedEvents"

  def getValidTrackingTags(date: LocalDate): Dataset[_] = {
    val customGoalTypeId = CustomGoalTypeId.get(task).get
    val includeInCustomGoal = IncludeInCustomGoal.get(task).get

    val adGroupDS = getActiveAdGroups(date)
    val campaignDS = CampaignDataSet().readLatestPartitionUpTo(date, true)
      .join(adGroupDS, Seq("CampaignId"), "left_semi")
    val ccrc = CampaignConversionReportingColumnDataSet().readLatestPartitionUpTo(date, true)

    val ccrcPreProcessed = ccrc
      .join(broadcast(campaignDS.select($"CampaignId", col(customGoalTypeId), $"CustomCPAClickWeight", $"CustomCPAViewthroughWeight")), Seq("CampaignId"), "left")

    val ccrcProcessed = task match {
      case "roas" => {
        ccrcPreProcessed.filter(col(customGoalTypeId) === 0 && $"ReportingColumnId" === 1).union(
          ccrcPreProcessed.filter(col(customGoalTypeId) > 0 && col(includeInCustomGoal))
            .filter((col(customGoalTypeId) === lit(1)) && ($"CustomROASWeight" =!= lit(0))
              or (col(customGoalTypeId) === lit(2)) && ($"CustomROASClickWeight" + $"CustomROASViewthroughWeight" =!= lit(0))
              or (col(customGoalTypeId) === lit(3)) && ($"CustomROASWeight" * ($"CustomROASClickWeight" + $"CustomROASViewthroughWeight") =!= lit(0))))
      }
      case _ => {
        ccrcPreProcessed.filter(col(customGoalTypeId) === 0 && $"ReportingColumnId" === 1).union(
          ccrcPreProcessed.filter(col(customGoalTypeId) > 0 && col(includeInCustomGoal))
            .filter((col(customGoalTypeId) === lit(1)) && ($"Weight" =!= lit(0))
              or (col(customGoalTypeId) === lit(2)) && ($"CustomCPAClickWeight" + $"CustomCPAViewthroughWeight" =!= lit(0)))
        )
      }
    }
    ccrcProcessed.select($"CampaignId", $"TrackingTagId", $"ReportingColumnId")
  }

  override def runTransform(args: Array[String]): Array[(String, Long)] = {
    val startDate = date.minusDays(conversionLookback)
    val validTags = getValidTrackingTags(date)
    val dailyAttr = DailyAttributionDataset().readDate(date)
      .withColumn("MonetaryValue", $"MonetaryValue".cast("decimal(18,6)"))
      .withColumn("CustomRevenue", $"CustomRevenue".cast("decimal(18,6)"))
      .filter($"AttributedEventLogEntryTime".isNotNull && ((unix_timestamp($"ConversionTrackerLogEntryTime") - unix_timestamp($"AttributedEventLogEntryTime")) <= Config.AttributionLookBack * 86400))
      .join(
        validTags.withColumnRenamed("ReportingColumnId", "CampaignReportingColumnId"),
        Seq("CampaignId", "TrackingTagId", "CampaignReportingColumnId"),
        "inner"
      )

    var rowCounts = new Array[(String, Long)](conversionLookback+1)
    // ImpDate [T-ConvLB, T]
    (0 to conversionLookback).map(i => {
      val ImpDate = startDate.plusDays(i)
      if (DailyBidFeedbackDataset().partitionExists(ImpDate) && DailyClickDataset().partitionExists(ImpDate)) {
        val dailyBF = DailyBidFeedbackDataset().readDate(ImpDate)
          .withColumnRenamed("BidFeedbackId", "AttributedEventId")
          .select("BidRequestId", "AttributedEventId", "AdGroupId", "CampaignId", "AdvertiserId")
        val dailyClick = DailyClickDataset().readDate(ImpDate)
          .withColumnRenamed("ClickRedirectId", "AttributedEventId")
          .select("BidRequestId", "AttributedEventId", "AdGroupId", "CampaignId", "AdvertiserId")
        // sometimes roas pixel could send back 0 value records.
        // all cases with valid MonetaryValue have valid MonetaryValueCurrency
        val invalidCurrency = Seq("Currency", "{Currency}", "{¥}")
        val dailyAttrEvents = dailyAttr.filter($"AttributedEventTypeId" === lit("2")).join(dailyBF, Seq("AttributedEventId", "AdGroupId", "CampaignId", "AdvertiserId"), "inner")
          .union(dailyAttr.filter($"AttributedEventTypeId" === lit("1")).join(dailyClick, Seq("AttributedEventId", "AdGroupId", "CampaignId", "AdvertiserId"), "inner"))
          .select("AdGroupId", "CampaignId", "AdvertiserId", "BidRequestId", "ConversionTrackerLogEntryTime", "MonetaryValue", "MonetaryValueCurrency", "CustomCPACount", "CustomRevenue")
          .withColumn("MonetaryValueCurrency", when($"MonetaryValueCurrency".isin(invalidCurrency: _*), null).otherwise($"MonetaryValueCurrency"))
          .withColumn("ConversionTrackerLogEntryTime", date_trunc(RoundUpTimeUnit, $"ConversionTrackerLogEntryTime"))
          .groupBy("AdGroupId", "CampaignId", "AdvertiserId", "BidRequestId", "ConversionTrackerLogEntryTime")
          .agg(
            sum("MonetaryValue").alias("MonetaryValue"),
            max("MonetaryValueCurrency").alias("MonetaryValueCurrency"),
            sum("CustomCPACount").alias("CustomCPACount"),
            sum("CustomRevenue").alias("CustomRevenue"),
          ).withColumn("Target", lit(1))

        val attrEventsByImpDate =
          task match {
            case "roas" => {
              val rate = DailyExchangeRateDataset().readDate(date).cache() // use scoreDate's exchange to simplify the calculation
              val campaign = CampaignDataSet().readLatestPartitionUpTo(date, true)
              dailyAttrEvents
                .withColumn("CurrencyCodeId", $"MonetaryValueCurrency").join(broadcast(rate), Seq("CurrencyCodeId"), "left")
                .join(broadcast(campaign.select("CampaignId", "CustomROASTypeId")), Seq("CampaignId"), "left")
                .withColumn("ValidRevenue", when($"CustomROASTypeId"===lit(0), $"MonetaryValue").otherwise($"CustomRevenue"))
                .withColumn("FromUSD", when($"MonetaryValueCurrency" === "USD", lit(1)).otherwise($"FromUSD"))
                .withColumn("RevenueInUSD", when($"ValidRevenue".isNotNull, $"ValidRevenue" / $"FromUSD").otherwise(0))
                .withColumn("Revenue", greatest($"RevenueInUSD", lit(1)))
                .selectAs[DailyAttributionEventsRecord]
            }
            case _ => dailyAttrEvents.withColumn("Revenue", lit(0))
              .selectAs[DailyAttributionEventsRecord]
          }

        // partitioned by AttrDate/ImpDate
        val attrRowCount = DailyAttributionEventsDataset().writePartition(attrEventsByImpDate, date, ImpDate.format(DefaultTimeFormatStrings.dateTimeFormatter), Some(1))

        rowCounts(i) = attrRowCount
        attrRowCount
      } else {
        rowCounts(i) = ("", 0L)
        rowCounts(i)
      }
    })

    rowCounts

  }
}
