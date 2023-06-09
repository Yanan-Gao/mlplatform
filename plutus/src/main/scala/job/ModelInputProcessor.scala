package job


import com.thetradedesk.geronimo.shared.loadModelFeatures
import com.thetradedesk.plutus.data.transform.TrainingDataTransform
import com.thetradedesk.logging.Logger
import com.thetradedesk.spark.TTDSparkContext.spark
import com.thetradedesk.spark.util.TTDConfig.config
import com.thetradedesk.spark.util.prometheus.PrometheusClient


import java.time.LocalDate


object ModelInputProcessor extends Logger {

  val ttdEnv = config.getString("ttd.env" , "dev")
  val date = config.getDate("date" , LocalDate.now())
  val daysOfDat = config.getInt("daysOfDat" , 1)
  val svNames = config.getStringSeq("svNames", Seq("google", "rubicon", "pubmatic"))
  val inputPath = config.getString("inputPath" , "s3://thetradedesk-mlplatform-us-east-1/users/nick.noone/pc")
  val inputPrefix = config.getString("inputPrefix" , "clean")

  val outputPath = config.getString("outputPath" , "s3://thetradedesk-mlplatform-us-east-1/users/nick.noone/pc")
  val outputPrefix = config.getString("outputPrefix" , "modelinput")

  val tfRecordPath = config.getString("tfRecordPath" , "tfrecord")
  val dims = config.getInt("dims" , 500000)

  val formats = config.getStringSeq("formats", Seq("tfrecord", "parquet"))

  val numCsvPartitions = config.getInt("numCsvPartitions", 20)
  val onlyWriteSingleDay = config.getBoolean("onlyWriteSingleDay", false)

  // Features json S3 location
  val featuresJson = config.getString("featuresJson", "s3://thetradedesk-mlplatform-us-east-1/features/data/plutus/v=1/dev/schemas/features.json")

  implicit val prometheus = new PrometheusClient("Plutus", "TrainingDataEtl")
  val jobDurationTimer = prometheus.createGauge("training_model_input_runtime", "Time to process 1 day of clean data in to model input data").startTimer()



  def main(args: Array[String]): Unit = {

    svNames.foreach { svName =>
      TrainingDataTransform.transform(
        s3Path = inputPath,
        ttdEnv = ttdEnv,
        inputS3Prefix = inputPrefix,
        outputS3Prefix = outputPrefix,
        svName = Some(svName),
        endDate = date,
        lookBack = Some(daysOfDat),
        formats = formats,
        featuresJson = featuresJson
      )
    }


    // clean up
    jobDurationTimer.setDuration()
    prometheus.pushMetrics()
    spark.close()
  }
}
