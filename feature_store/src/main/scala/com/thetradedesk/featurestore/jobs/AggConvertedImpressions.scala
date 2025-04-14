package com.thetradedesk.featurestore.jobs

import com.thetradedesk.featurestore._
import com.thetradedesk.featurestore.datasets._
import com.thetradedesk.featurestore.features.Features._
import com.thetradedesk.featurestore.transform.Merger.joinDataFrames
import com.thetradedesk.spark.TTDSparkContext.spark.implicits._
import com.thetradedesk.spark.util.TTDConfig.config
import org.apache.spark.sql.Dataset
import org.apache.spark.sql.functions._

import java.time.LocalDate


object AggConvertedImpressions extends FeatureStoreAggJob {
  override def jobName: String = "convertedimp"
  override def jobConfig = new FeatureStoreAggJobConfig( s"${getClass.getSimpleName.stripSuffix("$")}.json" )

  // input args
  val convLookback = config.getInt("convLookback", 15)

  override def loadInputData(date: LocalDate, lookBack: Int): Dataset[_] = {
    val convertedImp = ConvertedImpressionDataset(convLookback).readRange(date.minusDays(lookBack), date, isInclusive = true)
    convertedImp.withColumn("HourOfDay", hour($"LogEntryTime"))
      .withColumn("DayOfWeek", dayofweek($"LogEntryTime"))
      .withColumnRenamed("UIID", "TDID")

  }
}