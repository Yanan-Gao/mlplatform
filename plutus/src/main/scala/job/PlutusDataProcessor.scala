package job

import com.thetradedesk.logging.Logger
import com.thetradedesk.plutus.data.DATA_VERSION
import com.thetradedesk.plutus.data.transform.{PlutusDataTransform, RawDataTransform}
import com.thetradedesk.spark.TTDSparkContext.spark
import com.thetradedesk.spark.util.TTDConfig.config
import com.thetradedesk.spark.util.prometheus.PrometheusClient

import java.time.LocalDate


object PlutusDataProcessor extends Logger {

  val date = config.getDate("date", LocalDate.now())
  val outputPath = config.getString("outputPath", "s3://thetradedesk-mlplatform-us-east-1/features/data/plutus/v=2/")
  val dataVersion = config.getInt("dataVersion", DATA_VERSION)
  val rawOutputPrefix = config.getString("outputPrefix", "raw")
  val cleanOutputPrefix = config.getString("outputPrefix", "clean")
  val svNames = config.getStringSeq("svNames", Seq("google", "rubicon", "pubmatic"))

  val ttdEnv = config.getString("ttd.env", "dev")
  val outputTtdEnv = config.getStringOption("outputTtd.env")

  val partitions = config.getInt("partitions", 2000)
  val implicitSampleRate = config.getDouble("implicitSampleRate", 0.001)

  val mbtwRatio = config.getDouble("mbtwRatio", 2.0)

  implicit val prometheus = new PrometheusClient("Plutus", "TrainingDataEtl")
  val jobDurationTimer = prometheus.createGauge("training_data_raw_etl_runtime", "Time to process 1 day of bids, imppressions, lost bid data").startTimer()

  // Features json S3 location
  val featuresJson = config.getString("featuresJson", "s3://thetradedesk-mlplatform-us-east-1/features/data/plutus/v=1/dev/schemas/features.json")


  def main(args: Array[String]): Unit = {
    PlutusDataTransform.transform(
      date = date,
      svNames = svNames,
      partitions = partitions,
      outputPath = outputPath,
      rawOutputPrefix = rawOutputPrefix,
      cleanOutputPrefix = cleanOutputPrefix,
      inputTtdEnv = ttdEnv,
      dataVersion = dataVersion,
      implicitSampleRate = implicitSampleRate,
      maybeOutputTtdEnv = None,
      featuresJson = featuresJson,
    )

    // clean up
    jobDurationTimer.setDuration()
    prometheus.pushMetrics()
    spark.close()
  }
}

