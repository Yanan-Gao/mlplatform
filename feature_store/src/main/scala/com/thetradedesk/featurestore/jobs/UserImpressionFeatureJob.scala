package com.thetradedesk.featurestore.jobs

import com.thetradedesk.featurestore
import com.thetradedesk.featurestore.constants.FeatureConstants
import com.thetradedesk.featurestore.datasets.{UserImpressionFeature, UserImpressionFeatureDataset}
import com.thetradedesk.featurestore.{date, dateTime, shouldConsiderTDID}
import com.thetradedesk.geronimo.bidsimpression.schema.{BidsImpressions, BidsImpressionsSchema}
import com.thetradedesk.geronimo.shared.{GERONIMO_DATA_SOURCE, loadParquetData}
import com.thetradedesk.spark.TTDSparkContext.spark
import com.thetradedesk.spark.TTDSparkContext.spark.implicits._
import com.thetradedesk.spark.util.TTDConfig.config
import com.thetradedesk.spark.util.prometheus.PrometheusClient
import org.apache.spark.sql.SaveMode
import org.apache.spark.sql.functions._

/**
 * UserFrequencyFeatureJob is mainly for testing purpose
 * the job would generate user level features
 */
object UserImpressionFeatureJob {
  private implicit val prometheus = new PrometheusClient("FeatureStore", "UserImpressionFeatureJob")
  val jobRunningTime = prometheus.createGauge(s"user_impression_feature_job_running_time", "UserImpressionFeatureJob running time", "dateTime")

  object Config {
    val UserImpressionFeatureJobRotation = config.getInt("UserImpressionFeatureJobRotation", default = 1)
    val UserImpressionFeatureJobSalt = config.getString("UserImpressionFeatureJobSalt", default = "kXkeGQ")

  }

  def main(args: Array[String]): Unit = {
    val start = System.currentTimeMillis()
    runETLPipeline()
    jobRunningTime.labels(dateTime.toString).set(System.currentTimeMillis() - start)
    prometheus.pushMetrics()
  }

  def runETLPipeline(): Unit = {
    val bidImpressionsS3Path = BidsImpressions.BIDSIMPRESSIONSS3 + "prod/bidsimpressions/"

    val result = loadParquetData[BidsImpressionsSchema](bidImpressionsS3Path, date, lookBack = Some(0), source = Some(GERONIMO_DATA_SOURCE))
      .withColumnRenamed("UIID", "TDID")
      .where(shouldConsiderTDID('TDID, Config.UserImpressionFeatureJobRotation, Config.UserImpressionFeatureJobSalt, FeatureConstants.SecondsPerDay))
      .groupBy('TDID)
      .agg(count("*").cast("int").alias("seenCount1D"), avg('AdjustedBidCPMInUSD).cast("float").alias("avgCost1D"), sum('AdjustedBidCPMInUSD).cast("float").alias("totalCost1D"), max('FloorPriceInUSD).cast("float").alias("maxFloorPrice"))

    UserImpressionFeatureDataset().writePartition(
      result.as[UserImpressionFeature],
      date,
      saveMode = SaveMode.Overwrite
    )
  }
}
