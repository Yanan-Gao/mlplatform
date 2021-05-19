package job

import com.thetradedesk.logging.Logger
import com.thetradedesk.spark.TTDSparkContext.spark
import com.thetradedesk.spark.TTDSparkContext.spark.implicits._
import com.thetradedesk.spark.util.TTDConfig.config
import com.thetradedesk.spark.util.prometheus.PrometheusClient
import com.thetradedesk.data.{AdjustedImpressions, Bids, Discrepancy, GoogleLostBids, datePaddedPart, getParquetData}
import java.time.LocalDate

import com.thetradedesk.spark.sql.SQLFunctions.ColumnExtensions
import io.prometheus.client.Gauge
import org.apache.spark.sql.{DataFrame, SaveMode}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._


object TrainingData extends Logger {

  val date = config.getDate("date" , LocalDate.now())
  val lookBack = config.getInt("daysOfDat" , 1)
  val svName = config.getString("svName", "google")
  val outputPath = config.getString("outputPath" , "s3://thetradedesk-mlplatform-us-east-1/users/nick.noone/pc/trainingdata")

  val prometheus = new PrometheusClient("Plutus", "TrainingDataEtl")
  val jobDurationTimer = prometheus.createGauge("training_data_raw_etl_runtime", "Time to process 1 day of bids, imppressions, lost bid data").startTimer()
  val impressionsGauge: Gauge = prometheus.createGauge("raw_impressions_count" , "count of raw impressions")
  val bidsGauge = prometheus.createGauge("raw_bids_count", "count of raw bids")

  def main(args: Array[String]): Unit = {

    val glb = new GoogleLostBids
    val lostBids = glb.getLostBids(date, lookBack)(spark)

    val dis = new Discrepancy
    val (svbDf, pdaDf, dealDf) = dis.getDiscrepancyData(date, lookBack)(spark)

    val ai = new AdjustedImpressions
    val (imps, empDisDf) =  ai.getAdjustedImpressions(date, lookBack, svName, svbDf, pdaDf, dealDf.toDF, impressionsGauge)(spark)

    val b = new Bids

    val bids = b.getBidsData(date, lookBack, svName, svbDf, pdaDf, dealDf.toDF, empDisDf, bidsGauge)(spark)

    val bidsImpsDf = bids.join(imps, Seq("BidRequestId"), "left")

    val finalDf = bidsImpsDf.join(lostBids, Seq("BidRequestId"), "left")
      .withColumn("is_imp", when(col("RealMediaCost").isNotNull, 1.0).otherwise(0.0))

    // should i break this out by sv?
    val d = datePaddedPart(date)
    finalDf.write.mode(SaveMode.Overwrite).parquet(s"$outputPath/$svName/$d")

    // clean up
    jobDurationTimer.setDuration()
    prometheus.pushMetrics()
    spark.close()
  }

}
