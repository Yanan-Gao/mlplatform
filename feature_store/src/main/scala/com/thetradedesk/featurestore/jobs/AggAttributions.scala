package com.thetradedesk.featurestore.jobs

import com.thetradedesk.featurestore.datasets._
import com.thetradedesk.featurestore.features.Features._
import com.thetradedesk.featurestore.transform.Loader._
import com.thetradedesk.featurestore.{aggLevel, shouldTrackTDID}
import com.thetradedesk.spark.TTDSparkContext.spark
import com.thetradedesk.spark.TTDSparkContext.spark.implicits._
import com.thetradedesk.spark.sql.SQLFunctions.DataSetExtensions
import org.apache.spark.sql._
import org.apache.spark.sql.functions._

import java.time.LocalDate


object AggAttributions extends FeatureStoreAggJob {

  override val sourcePartition: String = "attribution"

  // todo: replace this part by config files
  override def catFeatSpecs: Array[CategoryFeatAggSpecs] = Array(
  )

  override def conFeatSpecs: Array[ContinuousFeatAggSpecs] = Array(
    ContinuousFeatAggSpecs(aggField = "ConversionTrackerId", aggWindow = 1, aggFunc = AggFunc.Count),
    ContinuousFeatAggSpecs(aggField = "ConversionTrackerId", aggWindow = 3, aggFunc = AggFunc.Count),
    ContinuousFeatAggSpecs(aggField = "ConversionTrackerId", aggWindow = 7, aggFunc = AggFunc.Count),
    ContinuousFeatAggSpecs(aggField = "Revenue", aggWindow = 1, aggFunc = AggFunc.Desc),
    ContinuousFeatAggSpecs(aggField = "Revenue", aggWindow = 3, aggFunc = AggFunc.Desc),
    ContinuousFeatAggSpecs(aggField = "Revenue", aggWindow = 7, aggFunc = AggFunc.Desc),
    ContinuousFeatAggSpecs(aggField = "ConvDelayInSeconds", aggWindow = 3, aggFunc = AggFunc.Desc),
    ContinuousFeatAggSpecs(aggField = "ConvDelayInSeconds", aggWindow = 7, aggFunc = AggFunc.Desc),
  )

  override def ratioFeatSpecs: Array[RatioFeatAggSpecs] = Array(
  )

  override def loadInputData(date: LocalDate, lookBack: Int): Dataset[_] = {
    val attributionDataset = DailyAttributionDataset()
    if(!attributionDataset.isProcessed(date)) {
      throw new RuntimeException(s"Success file not found for DailyAttributionDataset on date: ${date}, at location: ${attributionDataset.getSuccessFilePath(date)}")
    }
    val inputDf = attributionDataset.readPartition(date = date, lookBack = Some(lookBack)).filter(shouldTrackTDID(col(aggLevel)))
    // load CCRC table to filter down to attribution TrackingTags
    val ccrcProcessed = loadValidTrackingTag(date)

    val attInputDf = inputDf.join(broadcast(ccrcProcessed), Seq("CampaignId", "TrackingTagId"), "left_semi").selectAs[DailyAttributionRecord]

    attInputDf.withColumn("ConvDelayInSeconds", unix_timestamp(col("ConversionTrackerLogEntryTime")) - unix_timestamp(col("AttributedEventLogEntryTime")))
  }

}