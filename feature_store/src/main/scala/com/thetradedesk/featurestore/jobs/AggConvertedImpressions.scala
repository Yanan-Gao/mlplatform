package com.thetradedesk.featurestore.jobs

import com.thetradedesk.featurestore.datasets._
import com.thetradedesk.featurestore.{aggLevel, shouldTrackTDID}
import com.thetradedesk.spark.TTDSparkContext.spark
import com.thetradedesk.spark.TTDSparkContext.spark.implicits._
import com.thetradedesk.spark.util.TTDConfig.config
import org.apache.spark.sql.Dataset
import org.apache.spark.sql.functions._

import java.time.LocalDate


object AggConvertedImpressions extends FeatureStoreAggJob {

  override val sourcePartition: String = "convertedimp"

  // input args
  val convLookback = config.getInt("convLookback", 15)

  override def loadInputData(date: LocalDate, lookBack: Int): Dataset[_] = {
    val convertedImpDataset = ConvertedImpressionDataset(convLookback)
    if(!convertedImpDataset.isProcessed(date)) {
      throw new RuntimeException(s"Success file not found for ConvertedImpressionDataset on date: ${date} and lookback: ${convLookback}, at location: ${convertedImpDataset.getSuccessFilePath(date)}")
    }
    convertedImpDataset.readPartition(date = date, lookBack = Some(lookBack))
      .withColumn("HourOfDay", hour($"LogEntryTime"))
      .withColumn("DayOfWeek", dayofweek($"LogEntryTime"))
      .withColumnRenamed("UIID", "TDID")
      .filter(shouldTrackTDID(col(aggLevel)))

  }
}