package job.deals

import com.thetradedesk.plutus.data.envForRead
import com.thetradedesk.plutus.data.transform.virtualmaxbidbackoff.AdgroupMeanDealImpressionFloorTransform
import com.thetradedesk.spark.TTDSparkContext
import com.thetradedesk.spark.util.TTDConfig.config
import com.thetradedesk.spark.util.opentelemetry.OtelClient

import java.time.LocalDate

object AdgroupMeanDealImpressionFloorJob {

  val date: LocalDate = config.getDateRequired("date")
  val CTVSPENDTHRESHOLD = 0.5d
  val VARIABLESPENDTHRESHOLD =  0.99d

  val otelClient = new OtelClient("AdgroupMeanDealImpressionFloor", "AdgroupMeanDealImpressionFloorJob")
  val jobDurationGauge = otelClient.createGauge("adgroupmeandealimpressionfloor_run_time_seconds", "Job execution time in seconds")
  val numRowsWritten = otelClient.createGauge("adgroupmeandealimpressionfloor_num_rows", "Number of total rows written")

  def main(args: Array[String]): Unit = {
    val jobDurationGaugeTimer = jobDurationGauge.startTimer()
    
    val ds = AdgroupMeanDealImpressionFloorTransform.transform(date, envForRead = envForRead, VARIABLESPENDTHRESHOLD, CTVSPENDTHRESHOLD)
    
    val rowCount = ds.count()
    numRowsWritten.set(rowCount)
    
    jobDurationGaugeTimer.setDuration()
    otelClient.pushMetrics()
    TTDSparkContext.spark.stop()
  }
}
