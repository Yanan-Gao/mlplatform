package job

import com.thetradedesk.geronimo.bidsimpression.schema.{BidsImpressions, BidsImpressionsSchema}
import com.thetradedesk.plutus.data.{explicitDateTimePart, paddedDatePart}
import com.thetradedesk.spark.TTDSparkContext.spark
import com.thetradedesk.spark.TTDSparkContext.spark.implicits._
import com.thetradedesk.spark.datasets.core.S3Roots
import com.thetradedesk.spark.listener.WriteListener
import com.thetradedesk.spark.util.TTDConfig.{config, environment}
import com.thetradedesk.spark.util.prometheus.PrometheusClient

import java.time.{LocalDateTime, ZoneId}

object PcResultsGeronimoJob {

  val dateTime = config.getDateTime("date", LocalDateTime.now())
  val fileCount = config.getInt("fileCount", 768)

  def main(args: Array[String]): Unit = {
    val prometheus = new PrometheusClient("PCResultsLog", "PcResultsGeronimoJoin")
    val jobDurationGauge = prometheus.createGauge("pcresultsgeronimo_run_time_seconds", "Job execution time in seconds")
    val jobDurationGaugeTimer = jobDurationGauge.startTimer()
    val numRowsWritten = prometheus.createGauge("pcresultsgeronimo_num_rows", "Number of rows after join")
    val numRowsAbsent = prometheus.createGauge("pcresultsgeronimo_num_rows_absent", "Number of rows in pcResultsLog but absent in geronimo")

    // sample pcResultsPath: s3://ttd-identity/datapipeline/prod/pcresultslog/v=2/date=20231025/hour=1
    val pcResultsPath = f"${S3Roots.IDENTITY_ROOT}/prod/pcresultslog/v=2/date=${paddedDatePart(dateTime.toLocalDate)}/hour=${dateTime.getHour}"

    // sample geronimoPath: s3: //thetradedesk-mlplatform-us-east-1/features/data/koav4/v=1/prod/bidsimpressions/year=2023/month=10/day=25/hourPart=1
    val geronimoPath = f"${BidsImpressions.BIDSIMPRESSIONSS3}prod/bidsimpressions/${explicitDateTimePart(dateTime.atZone(ZoneId.of("UTC")))}"

    // sample outputPath: s3://ttd-identity/datapipeline/test/pcresultsgeronimo/v=2/date=20231025/hour=1
    val outputPath = f"${S3Roots.IDENTITY_ROOT}/${environment}/pcresultsgeronimo/v=2/date=${paddedDatePart(dateTime.toLocalDate)}/hour=${dateTime.getHour}"

    val geronimoDataset = spark.read.parquet(geronimoPath).drop("matchedSegments")
    val pcResultsDataset = spark.read.parquet(pcResultsPath).drop("LogEntryTime", "AdgroupId", "SupplyVendor", "PrivateContractId")

    val absentCount = pcResultsDataset.join(geronimoDataset, Seq("BidRequestId"), "leftanti").count()

    val outputDataset = geronimoDataset.join(pcResultsDataset, Seq("BidRequestId"), "left")

    val listener = new WriteListener()
    spark.sparkContext.addSparkListener(listener)

    outputDataset.coalesce(fileCount).write.parquet(outputPath)

    val rows = listener.rowsWritten
    println(s"Rows Written: $rows")

    jobDurationGaugeTimer.setDuration()
    numRowsWritten.set(rows)
    numRowsAbsent.set(absentCount)
    prometheus.pushMetrics()
    spark.stop()
  }
}