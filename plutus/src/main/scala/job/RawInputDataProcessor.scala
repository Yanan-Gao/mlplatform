package job


import com.thetradedesk.logging.Logger
import com.thetradedesk.spark.TTDSparkContext.spark
import com.thetradedesk.spark.util.TTDConfig.config
import com.thetradedesk.spark.util.prometheus.PrometheusClient
import io.prometheus.client.Gauge
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{Dataset, SaveMode}
import java.time.LocalDate

import com.thetradedesk.plutus.data.{AdjustedImpressions, Bids, Discrepancy, GoogleLostBids, explicitDatePart}
import com.thetradedesk.plutus.data.schema.GoogleMinimumBidToWinData



object RawInputDataProcessor extends Logger {

  val date = config.getDate("date" , LocalDate.now())

  val outputPath = config.getString("outputPath" , "s3://thetradedesk-mlplatform-us-east-1/users/nick.noone/pc/")
  val outputPrefix = config.getString("outputPrefix" , "raw")
  val svName = config.getString("svName", "google")
  val ttdEnv = config.getString("ttd.env" , "dev")

  val prometheus = new PrometheusClient("Plutus", "TrainingDataEtl")
  val jobDurationTimer = prometheus.createGauge("training_data_raw_etl_runtime", "Time to process 1 day of bids, imppressions, lost bid data").startTimer()
  val impressionsGauge: Gauge = prometheus.createGauge("raw_impressions_count" , "count of raw impressions")
  val bidsGauge = prometheus.createGauge("raw_bids_count", "count of raw bids")

  def main(args: Array[String]): Unit = {

    // TODO: should all this logic be moved into an object?
    val googleLostBidInfo: Dataset[GoogleMinimumBidToWinData] = GoogleLostBids.getLostBids(date)

    val (svbDf, pdaDf, dealDf) = Discrepancy.getDiscrepancyData(date)

    val (imps, empDisDf) =  AdjustedImpressions.getAdjustedImpressions(date, svName, svbDf, pdaDf, dealDf.toDF, impressionsGauge)

    val bids = Bids.getBidsData(date, svName, svbDf, pdaDf, dealDf.toDF, empDisDf, bidsGauge)

    val bidsImpsDf = bids.join(imps, Seq("BidRequestId"), "left")

    val rawInputDf = bidsImpsDf.join(googleLostBidInfo, Seq("BidRequestId"), "left")
      .withColumn("is_imp", when(col("RealMediaCost").isNotNull, 1.0).otherwise(0.0))

    // note the date part is year=yyyy/month=m/day=d/
    rawInputDf.write.mode(SaveMode.Overwrite)
      .parquet(s"$outputPath/$ttdEnv/$outputPrefix/$svName/${explicitDatePart(date)}")

    // clean up
    jobDurationTimer.setDuration()
    prometheus.pushMetrics()
    spark.close()
  }
}
