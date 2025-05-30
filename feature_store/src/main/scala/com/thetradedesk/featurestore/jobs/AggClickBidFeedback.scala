package com.thetradedesk.featurestore.jobs

import com.thetradedesk.featurestore.datasets._
import com.thetradedesk.spark.TTDSparkContext.spark
import org.apache.spark.sql._
import org.apache.spark.sql.functions._

import java.time.LocalDate


object AggClickBidFeedback extends FeatureStoreAggJob {

  override def loadInputData(date: LocalDate, lookBack: Int): Dataset[_] = {
    val inputDf = DailyClickBidFeedbackDataset().readPartition(date = date, lookBack = Some(lookBack))
    inputDf.withColumn("Click", when(col("ClickRedirectId").isNotNull, lit(1)).otherwise(0))
      .withColumn("Impression", when(col("BidFeedbackId").isNotNull, lit(1)).otherwise(0))
  }
}