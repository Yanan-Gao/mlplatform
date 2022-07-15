package job

import com.thetradedesk.geronimo.bidsimpression.schema.{BidsImpressions, BidsImpressionsSchema}
import com.thetradedesk.geronimo.shared.loadParquetData
import com.thetradedesk.philo.schema.{AdGroupFilterDataset, AdGroupFilterRecord, ClickTrackerDataset, ClickTrackerRecord}
import com.thetradedesk.philo.transform.ModelInputTransform
import com.thetradedesk.spark.util.TTDConfig.config
import com.thetradedesk.spark.TTDSparkContext.spark.implicits._
import com.thetradedesk.spark.TTDSparkContext.spark
import com.thetradedesk.philo.writeData

import java.time.LocalDate

  object ModelInput {
  val date = config.getDate("date" , LocalDate.now())
  val outputPath = config.getString("outputPath" , "s3://thetradedesk-mlplatform-us-east-1/features/data/philo/v=1/")
  val filterResults = config.getBoolean("filterResults", false)
  val outputPrefix = config.getString("outputPrefix" , "processed")
  val ttdEnv = config.getString("ttd.env" , "dev")
  val partitions = config.getInt("partitions", 2000)
  val filteredPartitions = config.getInt("filteredPartitions", 200)

  def main(args: Array[String]): Unit = {
    val brBfLoc = BidsImpressions.BIDSIMPRESSIONSS3 + f"${ttdEnv}/bidsimpressions/"

    val bidsImpressions = loadParquetData[BidsImpressionsSchema](brBfLoc, date, source = Some("geronimo"))
    val clicks = loadParquetData[ClickTrackerRecord](ClickTrackerDataset.CLICKSS3, date)

    if (filterResults) {
      val adGroupIds = spark.read.format("csv")
        .load(AdGroupFilterDataset.AGFILTERS3(ttdEnv))
        // single column is unnamed
        .withColumn("AdGroupId", $"_c0")
        .select($"AdGroupId").as[AdGroupFilterRecord]

      val (filteredData, labelCounts) = ModelInputTransform.transform(clicks, bidsImpressions, Some(adGroupIds), true)
      writeData(filteredData, outputPath, ttdEnv, "filtered", date, filteredPartitions)
      writeData(labelCounts, outputPath, ttdEnv, "filteredmetadata", date, 1, false)
    } else {
      val (trainingData, labelCounts) = ModelInputTransform.transform(clicks, bidsImpressions, None)
      writeData(trainingData, outputPath, ttdEnv, outputPrefix, date, partitions)
      writeData(labelCounts, outputPath, ttdEnv, "metadata", date, 1, false)
    }
  }


}
