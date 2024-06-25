package com.thetradedesk.featurestore.jobs

import com.thetradedesk.featurestore._
import com.thetradedesk.featurestore.datasets._
import com.thetradedesk.featurestore.features.Features._
import com.thetradedesk.featurestore.transform.Merger.joinDataFrames
import org.apache.spark.sql._
import org.apache.spark.sql.functions._

import java.time.LocalDate


object AggClickBidFeedback extends FeatureStoreAggJob {
  override def jobName: String = "clickbf"

  // todo: replace this part by config files
  override def catFeatSpecs: Array[CategoryFeatAggSpecs] = Array(
  )

  override def conFeatSpecs: Array[ContinuousFeatAggSpecs] = Array(
    ContinuousFeatAggSpecs(aggField = "SubmittedBidAmountInUSD", aggWindow = 1, aggFunc = AggFunc.Desc),
    ContinuousFeatAggSpecs(aggField = "SubmittedBidAmountInUSD", aggWindow = 3, aggFunc = AggFunc.Desc),
    ContinuousFeatAggSpecs(aggField = "SubmittedBidAmountInUSD", aggWindow = 7, aggFunc = AggFunc.Desc),
    ContinuousFeatAggSpecs(aggField = "Impression", aggWindow = 1, aggFunc = AggFunc.Count),
    ContinuousFeatAggSpecs(aggField = "Impression", aggWindow = 3, aggFunc = AggFunc.Count),
    ContinuousFeatAggSpecs(aggField = "Impression", aggWindow = 7, aggFunc = AggFunc.Count),
  )

  override def ratioFeatSpecs: Array[RatioFeatAggSpecs] = Array(
    RatioFeatAggSpecs(aggField = "Click", aggWindow = 1, denomField = "Impression", ratioMetrics = "CTR"),
    RatioFeatAggSpecs(aggField = "Click", aggWindow = 3, denomField = "Impression", ratioMetrics = "CTR"),
    RatioFeatAggSpecs(aggField = "Click", aggWindow = 7, denomField = "Impression", ratioMetrics = "CTR"),
    RatioFeatAggSpecs(aggField = "AdvertiserCostInUSD", aggWindow = 1, denomField = "Impression", ratioMetrics = "CPM"),
    RatioFeatAggSpecs(aggField = "AdvertiserCostInUSD", aggWindow = 3, denomField = "Impression", ratioMetrics = "CPM"),
    RatioFeatAggSpecs(aggField = "AdvertiserCostInUSD", aggWindow = 7, denomField = "Impression", ratioMetrics = "CPM"),
  )

  override def loadInputData(date: LocalDate, lookBack: Int): Dataset[_] = {
    val inputDf = DailyClickBidFeedbackDataset().readRange(date.minusDays(lookBack), date, isInclusive = true)
    inputDf.withColumn("Click", when(col("ClickRedirectId").isNotNull, lit(1)).otherwise(0))
      .withColumn("Impression", when(col("BidFeedbackId").isNotNull, lit(1)).otherwise(0))
  }
}