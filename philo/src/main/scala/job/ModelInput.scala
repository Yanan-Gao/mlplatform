package job

import com.thetradedesk.geronimo.bidsimpression.schema.{BidsImpressions, BidsImpressionsSchema}
import com.thetradedesk.geronimo.shared.loadParquetData
import com.thetradedesk.geronimo.FSUtils.fileExists
import com.thetradedesk.philo.schema.{ClickTrackerDataset, ClickTrackerRecord}
import com.thetradedesk.philo.transform.ModelInputTransform
import com.thetradedesk.spark.util.TTDConfig.config
import com.thetradedesk.spark.TTDSparkContext.spark.implicits._
import com.thetradedesk.spark.TTDSparkContext.spark
import com.thetradedesk.philo.writeData
import org.apache.spark.sql.DataFrame

import java.time.LocalDate

case class AdGroupFilterRecord(AdGroupId: String)
case class CountryFilterRecord(Country: String)

object ModelInput {
  val date = config.getDate("date", LocalDate.now())
  val outputPath = config.getString("outputPath", "s3://thetradedesk-mlplatform-us-east-1/features/data/philo/v=1/")
  val outputPrefix = config.getStringRequired("outputPrefix")
  val ttdEnv = config.getString("ttd.env", "dev")
  val partitions = config.getInt("partitions", 2000)
  val filterResults = config.getBoolean("filterResults", false)
  val filteredPartitions = config.getInt("filteredPartitions", 200)
  val filterFilePath = config.getString("filterFilePath", "")
  val filterBy = config.getString("filterBy", "AdGroupId")

  def main(args: Array[String]): Unit = {

    def loadFilterData(inputPath: String): DataFrame = {
      if (fileExists(inputPath)) {
        spark.read.format("csv")
          .load(inputPath)
          // single column is unnamed
          .withColumn(filterBy, $"_c0")
          .select(filterBy)
      } else {
        throw new Exception(f"Filter file does not exit at ${inputPath}")
      }
    }


    val brBfLoc = BidsImpressions.BIDSIMPRESSIONSS3 + f"${ttdEnv}/bidsimpressions/"

    val bidsImpressions = loadParquetData[BidsImpressionsSchema](brBfLoc, date, source = Some("geronimo"))
    val clicks = loadParquetData[ClickTrackerRecord](ClickTrackerDataset.CLICKSS3, date)

    if (filterResults) {
      val filterByData = loadFilterData(filterFilePath)

      val (filteredData, labelCounts) = filterBy match {
        case "AdGroupId" => ModelInputTransform.transform(clicks, bidsImpressions, Some(filterByData.as[AdGroupFilterRecord]), None, true)
        case "Country" => ModelInputTransform.transform(clicks, bidsImpressions, None, Some(filterByData.as[CountryFilterRecord]), true)
        case _ => throw new Exception(f"Cannot filter by property ${filterBy}")
      }

      writeData(filteredData, outputPath, ttdEnv, outputPrefix, date, filteredPartitions)
      writeData(labelCounts, outputPath, ttdEnv, outputPrefix + "metadata", date, 1, false)
    }

    if (!filterResults) {
      val (trainingData, labelCounts) = ModelInputTransform.transform(clicks, bidsImpressions, None, None)
      writeData(trainingData, outputPath, ttdEnv, outputPrefix, date, partitions)
      writeData(labelCounts, outputPath, ttdEnv, "metadata", date, 1, false)
    }
  }
}
