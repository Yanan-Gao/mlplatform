package job


import com.thetradedesk.bidsimpression.schema.{BidsImpressions, BidsImpressionsSchema}
import com.thetradedesk.plutus.data.transform.RawDataTransform
import com.thetradedesk.logging.Logger
import com.thetradedesk.spark.TTDSparkContext.spark
import com.thetradedesk.spark.TTDSparkContext.spark.implicits._
import com.thetradedesk.spark.util.TTDConfig.config
import com.thetradedesk.spark.util.prometheus.PrometheusClient
import com.thetradedesk.spark.sql.SQLFunctions._

import java.time.LocalDate
import com.thetradedesk.plutus.data.{cleansedDataPaths, explicitDatePart, loadParquetData}
import com.thetradedesk.plutus.data.schema.{Deals, DiscrepancyDataset, GoogleMinimumBidToWinData, GoogleMinimumBidToWinDataset, Pda, RawMBtoWinSchema, Svb}


object RawInputDataProcessor extends Logger {

  val date = config.getDate("date" , LocalDate.now())
  val outputPath = config.getString("outputPath" , "s3://thetradedesk-mlplatform-us-east-1/users/nick.noone/pc/")
  val outputPrefix = config.getString("outputPrefix" , "raw")
  val svNames = config.getStringSeq("svNames", Seq("google"))
  val ttdEnv = config.getString("ttd.env" , "dev")

  implicit val prometheus = new PrometheusClient("Plutus", "TrainingDataEtl")
  val jobDurationTimer = prometheus.createGauge("training_data_raw_etl_runtime", "Time to process 1 day of bids, imppressions, lost bid data").startTimer()

  def main(args: Array[String]): Unit = {

    val mbw = spark.read.format("csv")
      .option("sep", "\t")
      .option("header", "false")
      .option("inferSchema", "false")
      .option("mode", "DROPMALFORMED")
      .schema(GoogleMinimumBidToWinDataset.SCHEMA)
      .load(cleansedDataPaths(GoogleMinimumBidToWinDataset.S3PATH, date): _*)
      .selectAs[RawMBtoWinSchema]

    val svb = loadParquetData[Svb](DiscrepancyDataset.SBVS3, date)
    val pda = loadParquetData[Pda](DiscrepancyDataset.PDAS3, date)
    val deals = loadParquetData[Deals](DiscrepancyDataset.DEALSS3, date)

    val brBfLoc = BidsImpressions.BIDSIMPRESSIONSS3 + f"${ttdEnv}/bidsimpressions/"

    val bidsImpressions = loadParquetData[BidsImpressionsSchema](brBfLoc, date, source = Some("plutus"))

    RawDataTransform.transform(date, outputPath, ttdEnv, outputPrefix, svNames, bidsImpressions, mbw, (svb, pda, deals) )

    // clean up
    jobDurationTimer.setDuration()
    prometheus.pushMetrics()
    spark.close()
  }
}

