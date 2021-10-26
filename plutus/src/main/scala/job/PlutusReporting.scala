package job

import com.thetradedesk.spark.util.TTDConfig.config
import com.thetradedesk.spark.util.prometheus.PrometheusClient
import com.thetradedesk.plutus.data.transform.ReportingDataTransform
import com.thetradedesk.spark.TTDSparkContext.spark
import job.ModelInputProcessor.prometheus

import java.time.LocalDate

object PlutusReporting {
  val date = config.getDate("date" , LocalDate.now()) //TODO: note this can cause bad things. Date should be required and not linked ot current date.
  val lookBack = config.getInt("daysOfDat" , 1)
  val model = config.getString("model", "plutus")
  val dataCenters = config.getStringSeq("dataCenters", Seq("ca2", "de1"))
  val testIdStringMatch = config.getString("testIdStringMatch" , "PCM(tfmodel")

  implicit val prometheus = new PrometheusClient("Plutus", "Reporting")

  def main(args: Array[String]): Unit = {

    ReportingDataTransform.transform(date, model, testIdStringMatch, dataCenters)

    prometheus.pushMetrics()
    spark.close()

  }

}
