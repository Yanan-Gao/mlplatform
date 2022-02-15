package com.ttd.contextual.features.web.urlprofile

import java.time.LocalDate

import com.swoop.alchemy.spark.expressions.hll.functions.hll_init_agg
import com.ttd.contextual.datasets.generated.contextual.web.urlprofile.{AvailsTrafficCount, AvailsTrafficCountRecord}
import com.ttd.contextual.util.elDoradoUtilities.datasets.sources.Avails7DayDataSet
import com.ttd.contextual.util.elDoradoUtilities.spark.TTDConfig.config
import com.ttd.contextual.spark.TTDSparkContext.spark.implicits._
import org.apache.spark.sql.functions.{count, explode}

class TrafficCountConfig {
  // parameters for all
  val ApplicationName = "PageHtmlSourceExtraction"
  val Environment: String = config.getString("ttd.env", "local")
  val date: LocalDate = config.getDate("date", LocalDate.now().minusDays(2))
  val relativeSD: Double = config.getDouble("relativeSD", 0.05)

  // Parallelism parameters
  val fileCount: Int = config.getInt("fileCount", 100)
}

/* Pre-aggregate traffic and distinct counts at the URL level */
object TrafficCountWorkflow {
  def run[T<:TrafficCountConfig](config: TrafficCountConfig): Unit = {
    val date = config.date

    val ds = Avails7DayDataSet()
      .readDate(date)
      .select('date, explode('Urls) as "url", 'TdidGuid)
      .groupBy("date", "url")
      .agg(hll_init_agg('TdidGuid, relativeSD = config.relativeSD) as "hllTDIDsketch", count("*") as "count")
      .as[AvailsTrafficCountRecord]

    AvailsTrafficCount().writePartition(ds, date, config.fileCount, "")
  }

  def main(args: Array[String]): Unit = {
    run(new TrafficCountConfig)
  }
}
