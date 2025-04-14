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
  override def jobConfig = new FeatureStoreAggJobConfig( s"${getClass.getSimpleName.stripSuffix("$")}.json" )

  override def loadInputData(date: LocalDate, lookBack: Int): Dataset[_] = {
    val inputDf = DailyClickBidFeedbackDataset().readRange(date.minusDays(lookBack), date, isInclusive = true)
    inputDf.withColumn("Click", when(col("ClickRedirectId").isNotNull, lit(1)).otherwise(0))
      .withColumn("Impression", when(col("BidFeedbackId").isNotNull, lit(1)).otherwise(0))
  }
}