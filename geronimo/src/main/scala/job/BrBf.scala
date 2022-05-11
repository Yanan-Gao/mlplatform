package job

import com.thetradedesk.geronimo.bidsimpression.schema.{ContextualCategoryDataset, ContextualCategoryRecord}
import com.thetradedesk.geronimo.bidsimpression.transform.BidsImpressions
import com.thetradedesk.geronimo.bidsimpression.transform.BidsImpressions.writeOutput
import com.thetradedesk.geronimo.shared.{loadParquetData, loadParquetDataHourly}
import com.thetradedesk.geronimo.shared.schemas.{AdvertiserDataset, AdvertiserRecord, BidFeedbackDataset, BidFeedbackRecord, BidRequestDataset, BidRequestRecord}
import com.thetradedesk.spark.TTDSparkContext.spark
import com.thetradedesk.spark.TTDSparkContext.spark.implicits._
import com.thetradedesk.spark.util.TTDConfig.config
import com.thetradedesk.spark.util.prometheus.PrometheusClient

import java.time.LocalDate

object BrBf {

  val date = config.getDate("date" , LocalDate.now())
  val outputPath = config.getString("outputPath" , "s3://thetradedesk-mlplatform-us-east-1/features/data/koav4/v=1/")
  val outputPrefix = config.getString("outputPrefix" , "bidsimpressions")
  val ttdEnv = config.getString("ttd.env" , "dev")
  val writePartitions = config.getInt("writePartitions", 2000)
  val numberOfPartitionBucket = config.getInt("numberOfPartitionBucket", 100)
  val numberOfPopularUrl = config.getInt("numberOfPopularUrl", 200)

  val hours = config.getStringSeqRequired("hours")

  implicit val prometheus = new PrometheusClient("Plutus", "TrainingDataEtl")
  val jobDurationTimer = prometheus.createGauge("training_data_bids_imps_join", "Time to process 1 day of bids, impressions").startTimer()
  val contextualCoverageGauge = prometheus.createGauge("contextual_coverage", "Contextual data coverage rate")

  def main(args: Array[String]): Unit = {

    val inputHours = hours.map(_.toInt)

    val impressions = loadParquetDataHourly[BidFeedbackRecord](s3path=BidFeedbackDataset.BFS3, date, inputHours)
    val bids = loadParquetDataHourly[BidRequestRecord](BidRequestDataset.BIDSS3, date, inputHours)
    val adv = loadParquetData[AdvertiserRecord](AdvertiserDataset.ADVS3, date)
    val contextual = loadParquetData[ContextualCategoryRecord](s3path = ContextualCategoryDataset.contextual, date)

    val (bfBf, metrics) = BidsImpressions.transform(bids, impressions, adv, contextual, numberOfPartitionBucket, numberOfPopularUrl)

    writeOutput(bfBf, outputPath, ttdEnv, outputPrefix, date, inputHours, writePartitions)


    // clean up
    contextualCoverageGauge.set(metrics.ContextualCoverage)
    jobDurationTimer.setDuration()
    prometheus.pushMetrics()
    spark.close()
  }

}
