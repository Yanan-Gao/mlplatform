package job

import com.thetradedesk.geronimo.bidsimpression.schema.{BidsImpressions, BidsImpressionsSchema}
import com.thetradedesk.geronimo.shared.loadParquetData
import com.thetradedesk.philo.schema.{ClickTrackerDataset, ClickTrackerRecord}
import com.thetradedesk.philo.transform.ModelInputTransform
import com.thetradedesk.philo.transform.ModelInputTransform.writeTfRecords
import com.thetradedesk.spark.util.TTDConfig.config
import com.thetradedesk.spark.TTDSparkContext.spark.implicits._
import com.thetradedesk.spark.TTDSparkContext.spark

import java.time.LocalDate

  object ModelInput {
  val date = config.getDate("date" , LocalDate.now())
  val outputPath = config.getString("outputPath" , "s3://thetradedesk-mlplatform-us-east-1/features/data/philo/v=1/")
  val outputPrefix = config.getString("outputPrefix" , "processed")
  val ttdEnv = config.getString("ttd.env" , "dev")
  val partitions = config.getInt("partitions", 2000)

  def main(args: Array[String]): Unit = {


    val brBfLoc = BidsImpressions.BIDSIMPRESSIONSS3 + f"${ttdEnv}/bidsimpressions/"

    val bidsImpressions = loadParquetData[BidsImpressionsSchema](brBfLoc, date, source = Some("geronimo"))
    val clicks = loadParquetData[ClickTrackerRecord](ClickTrackerDataset.CLICKSS3, date)

    val trainingData = ModelInputTransform.transform(clicks, bidsImpressions)

    writeTfRecords(trainingData, outputPath, ttdEnv, outputPrefix, date, partitions)

  }


}
