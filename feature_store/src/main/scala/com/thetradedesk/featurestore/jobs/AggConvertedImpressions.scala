package com.thetradedesk.featurestore.jobs

import com.thetradedesk.featurestore.datasets._
import com.thetradedesk.spark.TTDSparkContext.spark
import com.thetradedesk.spark.TTDSparkContext.spark.implicits._
import com.thetradedesk.spark.util.TTDConfig.config
import org.apache.spark.sql.Dataset
import org.apache.spark.sql.functions._

import java.time.LocalDate


object AggConvertedImpressions extends FeatureStoreAggJob {

  // input args
  val convLookback = config.getInt("convLookback", 15)

  override def loadInputData(date: LocalDate, lookBack: Int): Dataset[_] = {
    val convertedImp = ConvertedImpressionDataset(convLookback).readPartition(date = date, lookBack = Some(lookBack))
    convertedImp.withColumn("HourOfDay", hour($"LogEntryTime"))
      .withColumn("DayOfWeek", dayofweek($"LogEntryTime"))
      .withColumnRenamed("UIID", "TDID")

  }
}