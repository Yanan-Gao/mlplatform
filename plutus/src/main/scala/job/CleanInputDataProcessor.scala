package job


import com.thetradedesk.plutus.data.transform.CleanInputDataTransform
import com.thetradedesk.spark.TTDSparkContext.spark
import com.thetradedesk.spark.util.TTDConfig.config
import com.thetradedesk.spark.util.prometheus.PrometheusClient

import java.time.LocalDate


object CleanInputDataProcessor {
  val date = config.getDate("date" , LocalDate.now()) //TODO: note this can cause bad things. Date should be required and not linked ot current date.
  val lookBack = config.getInt("daysOfDat" , 1)

  val inputPath = config.getString("inputPath" , "s3://thetradedesk-mlplatform-us-east-1/features/data/plutus/v=1/")
  val outputPath = config.getString("outputPath" , "s3://thetradedesk-mlplatform-us-east-1/features/data/plutus/v=1/")
  val inputPrefix = config.getString("inputPrefix" , "raw")
  val outputPrefix = config.getString("outputPrefix" , "clean")
  val svNames = config.getStringSeq("svName", Seq("google", "rubicon", "pubmatic"))
  val extremeValueThreshold = config.getDouble("mbwRatio" , 0.8)
  val ttdEnv = config.getString("ttd.env" , "dev")


  implicit val prometheus = new PrometheusClient("Plutus", "TrainingDataEtl")
  val jobDurationTimer = prometheus.createGauge("clean_data_job_duration", "Time to process 1 day of clean data").startTimer()



  def main(args: Array[String]): Unit  = {
    svNames.foreach { svName =>
      CleanInputDataTransform.transform(date, ttdEnv, inputPath, inputPrefix, extremeValueThreshold, Some(svName), outputPath, outputPrefix)
    }
    // clean up
    jobDurationTimer.setDuration()
    prometheus.pushMetrics()
    spark.close()
  }
}
