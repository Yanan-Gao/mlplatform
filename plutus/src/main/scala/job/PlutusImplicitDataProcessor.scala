package job

import com.thetradedesk.logging.Logger
import com.thetradedesk.plutus.data.DATA_VERSION
import com.thetradedesk.plutus.data.transform.PlutusDataTransform
import com.thetradedesk.spark.TTDSparkContext.spark
import com.thetradedesk.spark.util.TTDConfig.config
import com.thetradedesk.spark.util.prometheus.PrometheusClient

import java.time.LocalDate


object PlutusImplicitDataProcessor extends Logger {

  val date = config.getDate("date", LocalDate.now())
  val outputPath = config.getString("outputPath", "s3://thetradedesk-mlplatform-us-east-1/features/data/plutus/")
  val dataVersion = config.getInt("dataVersion", DATA_VERSION)
  val cleanOutputPrefix = config.getString("outputPrefix", "clean")

  val partitions = config.getInt("partitions", 500)
  val facetPartitions = config.getIntOption("facetPartitions")

  val implicitSampleRate = config.getDouble("implicitSampleRate", 0.1)

  implicit val prometheus = new PrometheusClient("Plutus", "TrainingDataImEtl")
  val jobDurationTimer = prometheus.createGauge("plutus_data_proc_im_etl_runtime", "Time to process 1 day of bids, impressions, lost bid data").startTimer()

  // Features json S3 location
  val featuresJson = config.getString("featuresJson", "s3://thetradedesk-mlplatform-us-east-1/features/data/plutus/v=1/dev/schemas/features.json")


  def main(args: Array[String]): Unit = {
    PlutusDataTransform.processImplicit(
      date = date,
      partitions = partitions,
      maybeFacetPartitions=facetPartitions,
      outputPath = outputPath,
      cleanOutputPrefix = cleanOutputPrefix,
      dataVersion = dataVersion,
      implicitSampleRate = implicitSampleRate,
      featuresJson = featuresJson
    )

    // clean up
    jobDurationTimer.setDuration()
    prometheus.pushMetrics()
    spark.close()
  }
}

