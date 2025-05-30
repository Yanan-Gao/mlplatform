package com.thetradedesk.featurestore.jobs

import com.thetradedesk.featurestore._
import com.thetradedesk.featurestore.constants.FeatureConstants._
import com.thetradedesk.featurestore.datasets._
import com.thetradedesk.spark.TTDSparkContext.spark.implicits._
import com.thetradedesk.spark.sql.SQLFunctions.DataSetExtensions
import org.apache.spark.sql.Dataset
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._


/** Generate daily attribution for CPA and Roas Campaigns with revenue calculated
 *
 */
object GenDailyAttribution extends FeatureStoreGenJob[DailyAttributionRecord] {

  override def initDataSet(): DailyAttributionDataset = {
    DailyAttributionDataset()
  }

  override def generateDataSet(): Dataset[DailyAttributionRecord] = {

    val attributedEvent = AttributedEventDataSet().readDate(date)
      .filter($"AttributedEventTypeId".isin(List(AttributedEventTypeId_Click, AttributedEventTypeId_View): _*))
      .withColumn("AttributedEventLogEntryTime", to_timestamp(col("AttributedEventLogEntryTime")).as("AttributedEventLogEntryTime"))
      .selectAs[AttributedEventRecord]


    val attributedEventResult = AttributedEventResultDataSet().readDate(date)
      .filter($"AttributionMethodId".isin(List(AttributionMethodId_LastClick, AttributionMethodId_ViewThru): _*))
      .withColumn("ConversionTrackerLogEntryTime", to_timestamp(col("ConversionTrackerLogEntryTime")).as("ConversionTrackerLogEntryTime"))
      .selectAs[AttributedEventResultRecord]

    val rate = CurrencyExchangeRateDataSet().readLatestPartitionUpTo(date, isInclusive = true)
      .withColumn("RecencyRank", dense_rank().over(Window.partitionBy($"CurrencyCodeId").orderBy($"AsOfDateUTC".desc)))
      .filter($"RecencyRank"===lit(1))
      .select(
        $"CurrencyCodeId",
        $"FromUSD",
        $"AsOfDateUTC"
      ).orderBy($"CurrencyCodeId".asc)
      .selectAs[DailyExchangeRateRecord]

    val campaignROIGoal = CampaignROIGoalDataSet().readLatestPartitionUpTo(date, true).selectAs[CampaignROIGoalRecord]
      .filter(($"Priority" === 1) && ($"ROIGoalTypeId".isin(List(ROIGoalTypeId_CPA, ROIGoalTypeId_ROAS): _*)))

    val dailyAttribution = attributedEvent.join(attributedEventResult,
      Seq("ConversionTrackerLogFileId", "ConversionTrackerIntId1", "ConversionTrackerIntId2", "AttributedEventLogFileId", "AttributedEventIntId1", "AttributedEventIntId2"),
      "inner"
    ).withColumn("CurrencyCodeId", $"MonetaryValueCurrency")
      .join(broadcast(rate), Seq("CurrencyCodeId"), "left")
      .join(broadcast(campaignROIGoal), Seq("CampaignId"), "left")
      .withColumn("ValidRevenue", $"MonetaryValue".isNotNull && $"MonetaryValueCurrency".isNotNull)
      .withColumn("FromUSD", when($"MonetaryValueCurrency" === "USD", lit(1)).otherwise($"FromUSD"))
      .withColumn("RevenueInUSD", when($"ValidRevenue", $"MonetaryValue" / $"FromUSD").otherwise(0))
      .withColumn("Revenue", when($"ROIGoalTypeId" === ROIGoalTypeId_ROAS, greatest($"RevenueInUSD", lit(1))).otherwise(lit(0))) // only keep revenue for roas campaigns
      .selectAs[DailyAttributionRecord]

    dailyAttribution
  }
}
