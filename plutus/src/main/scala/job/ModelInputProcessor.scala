package job

import com.thetradedesk.data.schema.GoogleMinimumBidToWinDataset
import com.thetradedesk.data._
import com.thetradedesk.logging.Logger
import com.thetradedesk.spark.TTDSparkContext.spark
import com.thetradedesk.spark.util.TTDConfig.config
import com.thetradedesk.spark.util.prometheus.PrometheusClient
import io.prometheus.client.Gauge
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{Dataset, SaveMode}

import java.time.LocalDate


object ModelInputProcessor extends Logger {

  val date = config.getDate("date" , LocalDate.now())
  val lookBack = config.getInt("daysOfDat" , 1)
  val svName = config.getString("svName", "google")
  val outputPath = config.getString("outputPath" , "s3://thetradedesk-mlplatform-us-east-1/users/nick.noone/pc/trainingdata")

  val prometheus = new PrometheusClient("Plutus", "TrainingDataEtl")
  val jobDurationTimer = prometheus.createGauge("training_data_raw_etl_runtime", "Time to process 1 day of bids, imppressions, lost bid data").startTimer()
  val impressionsGauge: Gauge = prometheus.createGauge("raw_impressions_count" , "count of raw impressions")
  val bidsGauge = prometheus.createGauge("raw_bids_count", "count of raw bids")

  def main(args: Array[String]): Unit = {

    //TODO: Read in clean data (last N days)

    //TODO: Split into train/test/validation (configurable percentages or days)
    // eg: lookback = 10 --> train [-9, -2] val [-1] test [0]
    // or collapse into one large dataframe and split(0.8, 0.1, 0.1) but this will lose the temporal nature of the data
    // compromise might be to hold-out test to lookback day [0] then collapse [-9,-1] and split (0.9, 0.1)

    //TODO: TFRecord. Either have it create TF record from the train/test/val dataframe or construct the train/val/test
    // from the TFRecord files.

    // clean up
    jobDurationTimer.setDuration()
    prometheus.pushMetrics()
    spark.close()
  }
}
