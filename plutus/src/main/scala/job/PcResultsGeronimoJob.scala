package job

import com.thetradedesk.plutus.data.transform.PcResultsGeronimoTransform
import com.thetradedesk.spark.TTDSparkContext
import com.thetradedesk.spark.util.TTDConfig.config
import com.thetradedesk.spark.util.prometheus.PrometheusClient
import org.apache.spark.sql.SparkSession

import java.time.LocalDateTime

object PcResultsGeronimoJob {

  val dateTime = config.getDateTime("date", LocalDateTime.now())
  val fileCount = config.getInt("fileCount", 768)
  val ttdEnv = config.getString("ttd.env", "dev")

  val prometheus = new PrometheusClient("PCResultsLog", "PcResultsGeronimoJoin")
  val jobDurationGauge = prometheus.createGauge("pcresultsgeronimo_run_time_seconds", "Job execution time in seconds")
  val numRowsWritten = prometheus.createGauge("pcresultsgeronimo_num_rows", "Number of rows after join")
  val numRowsAbsent = prometheus.createGauge("pcresultsgeronimo_num_rows_absent", "Number of BidRequestIds in other datasets but absent in geronimo", labelNames = "source")

  // In test mode, we will setup a local spark context
  val spark = if (ttdEnv == "local") {
    SparkSession.builder().master("local[*]").getOrCreate()
  } else {
    TTDSparkContext.spark
  }

  // sample pcResultsPath: s3://ttd-identity/datapipeline/prod/pcresultslog/v=2/date=20231025/hour=1
  // sample geronimoPath: s3: //thetradedesk-mlplatform-us-east-1/features/data/koav4/v=1/prod/bidsimpressions/year=2023/month=10/day=25/hourPart=1
  // sample outputPath: s3://ttd-identity/datapipeline/test/pcresultsgeronimo/v=2/date=20231025/hour=1

  def main(args: Array[String]): Unit = {
    val jobDurationGaugeTimer = jobDurationGauge.startTimer()

    PcResultsGeronimoTransform.transform(dateTime, fileCount, ttdEnv = Some(ttdEnv))

    jobDurationGaugeTimer.setDuration()
    prometheus.pushMetrics()
    spark.stop()
  }
}