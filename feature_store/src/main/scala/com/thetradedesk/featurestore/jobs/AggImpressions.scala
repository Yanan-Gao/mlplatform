package com.thetradedesk.featurestore.jobs

import com.thetradedesk.featurestore.datasets._
import com.thetradedesk.geronimo.bidsimpression.schema.{BidsImpressions, BidsImpressionsSchema}
import com.thetradedesk.geronimo.shared.{GERONIMO_DATA_SOURCE, loadParquetData}
import com.thetradedesk.spark.TTDSparkContext.spark.implicits._
import com.thetradedesk.spark.sql.SQLFunctions.{ColumnExtensions, DataSetExtensions}
import org.apache.spark.sql.Dataset
import org.apache.spark.sql.functions._

import java.time.LocalDate


object AggImpressions extends FeatureStoreAggJob {
  override def jobName: String = "impressions"
  override def jobConfig = new FeatureStoreAggJobConfig( s"${getClass.getSimpleName.stripSuffix("$")}.json" )

  override def loadInputData(date: LocalDate, lookBack: Int): Dataset[_] = {
    // load impressions from geronimo dataset
    val bidImpressionsS3Path = BidsImpressions.BIDSIMPRESSIONSS3 + "prod/bidsimpressions/"
    val bidsImpressions = loadParquetData[BidsImpressionsSchema](bidImpressionsS3Path, date, lookBack = Some(lookBack), source = Some(GERONIMO_DATA_SOURCE))
    bidsImpressions.filter($"IsImp")
      .withColumn("AdFormat", concat(col("AdWidthInPixels"), lit('x'), col("AdHeightInPixels")))
      .withColumn("IsTracked", when($"UIID".isNotNullOrEmpty && $"UIID" =!= lit("00000000-0000-0000-0000-000000000000"), lit(1)).otherwise(0))
      .withColumn("RenderingContext", $"RenderingContext.value")
      .withColumn("DeviceType", $"DeviceType.value")
      .withColumn("OperatingSystem", $"OperatingSystem.value")
      .withColumn("OperatingSystemFamily", $"OperatingSystemFamily.value")
      .withColumn("Browser", $"Browser.value")
      .withColumn("InternetConnectionType", $"InternetConnectionType.value")
      .selectAs[FeatureBidsImpression]
      .withColumn("HourOfDay", hour($"LogEntryTime"))
      .withColumn("DayOfWeek", dayofweek($"LogEntryTime"))
      .withColumnRenamed("UIID", "TDID")
  }
}