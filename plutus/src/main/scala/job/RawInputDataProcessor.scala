package job


import com.thetradedesk.geronimo.bidsimpression.schema.{BidsImpressions, BidsImpressionsSchema}
import com.thetradedesk.plutus.data.transform.RawDataTransform
import com.thetradedesk.logging.Logger
import com.thetradedesk.spark.TTDSparkContext.spark
import com.thetradedesk.spark.TTDSparkContext.spark.implicits._
import com.thetradedesk.spark.util.TTDConfig.config
import com.thetradedesk.spark.util.prometheus.PrometheusClient
import com.thetradedesk.spark.sql.SQLFunctions._

import java.time.LocalDate
import com.thetradedesk.plutus.data.{cleansedDataPaths, explicitDatePart, loadCsvData, loadParquetData}
import com.thetradedesk.plutus.data.schema.{Deals, DiscrepancyDataset, Pda, RawLostBidData, RawLostBidDataset, Svb}
import org.apache.spark.sql.functions.col


object RawInputDataProcessor extends Logger {

  val date = config.getDate("date", LocalDate.now())
  val outputPath = config.getString("outputPath", "s3://thetradedesk-mlplatform-us-east-1/users/nick.noone/pc/")
  val outputPrefix = config.getString("outputPrefix", "raw")
  val svNames = config.getStringSeq("svNames", Seq("google", "rubicon"))
  val ttdEnv = config.getString("ttd.env", "dev")
  val partitions = config.getInt("partitions", 2000)

  implicit val prometheus = new PrometheusClient("Plutus", "TrainingDataEtl")
  val jobDurationTimer = prometheus.createGauge("training_data_raw_etl_runtime", "Time to process 1 day of bids, imppressions, lost bid data").startTimer()

  def main(args: Array[String]): Unit = {
    val rawLostBidData = loadCsvData[RawLostBidData](RawLostBidDataset.S3PATH, date, RawLostBidDataset.SCHEMA)


    val svb = loadParquetData[Svb](DiscrepancyDataset.SBVS3, date)
    val pda = loadParquetData[Pda](DiscrepancyDataset.PDAS3, date)
    val deals = loadParquetData[Deals](DiscrepancyDataset.DEALSS3, date)

    val brBfLoc = BidsImpressions.BIDSIMPRESSIONSS3 + f"${ttdEnv}/bidsimpressions/"

    val bidsImpressions = loadParquetData[BidsImpressionsSchema](brBfLoc, date, source = Some("plutus"))
      .filter(col("AuctionType") === 1 && $"SupplyVendor".isin(svNames: _*)) // Note the difference with Impression Data


    val rawDataDf = RawDataTransform.transform(date, svNames, bidsImpressions, rawLostBidData, (svb, pda, deals), partitions)

    svNames.foreach { sv =>
      RawDataTransform.writeOutput(rawDataDf.filter($"SupplyVendor" === sv), outputPath, ttdEnv, outputPrefix, sv, date)
    }

    // clean up
    jobDurationTimer.setDuration()
    prometheus.pushMetrics()
    spark.close()
  }
}

