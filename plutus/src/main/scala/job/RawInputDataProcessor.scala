package job

import com.thetradedesk.data.transform.RawDataTransform
import com.thetradedesk.logging.Logger
import com.thetradedesk.spark.TTDSparkContext.spark
import com.thetradedesk.spark.util.TTDConfig.config
import com.thetradedesk.spark.util.prometheus.PrometheusClient

import java.time.LocalDate


object RawInputDataProcessor extends Logger {

  val date = config.getDate("date" , LocalDate.now())
  val outputPath = config.getString("outputPath" , "s3://thetradedesk-mlplatform-us-east-1/users/nick.noone/pc/")
  val outputPrefix = config.getString("outputPrefix" , "raw")
  val svName = config.getString("svName", "google")
  val ttdEnv = config.getString("ttd.env" , "dev")

  implicit val prometheus = new PrometheusClient("Plutus", "TrainingDataEtl")
  val jobDurationTimer = prometheus.createGauge("training_data_raw_etl_runtime", "Time to process 1 day of bids, imppressions, lost bid data").startTimer()

  def main(args: Array[String]): Unit = {

    RawDataTransform.transform(date, outputPath, ttdEnv, outputPrefix, svName)

    // clean up
    jobDurationTimer.setDuration()
    prometheus.pushMetrics()
    spark.close()
  }
}
