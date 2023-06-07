package job

import com.thetradedesk.geronimo.bidsimpression.schema.{BidsImpressions, BidsImpressionsSchema}
import com.thetradedesk.geronimo.shared.{loadModelFeatures, loadParquetData}
import com.thetradedesk.philo.schema.{AdGroupPerformanceModelValueDataset, AdGroupPerformanceModelValueRecord, ClickTrackerDataset, ClickTrackerRecord}
import com.thetradedesk.philo.transform.ModelInputTransform
import com.thetradedesk.spark.util.TTDConfig.config
import com.thetradedesk.spark.TTDSparkContext.spark.implicits._
import com.thetradedesk.spark.TTDSparkContext.spark
import com.thetradedesk.philo.writeData
import com.thetradedesk.spark.TTDSparkContext
import com.thetradedesk.spark.util.io.FSUtils
import com.thetradedesk.spark.util.io.FSUtils.fileExists
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
  val featuresJson = config.getString("featuresJson", default="s3://thetradedesk-mlplatform-us-east-1/features/data/philo/v=1/prod/schemas/features.json")

  def main(args: Array[String]): Unit = {
    val readEnv = if (ttdEnv == "prodTest") "prod" else ttdEnv
    val writeEnv = if (ttdEnv == "prodTest") "dev" else ttdEnv

    val brBfLoc = BidsImpressions.BIDSIMPRESSIONSS3 + f"${readEnv}/bidsimpressions/"

    val bidsImpressions = loadParquetData[BidsImpressionsSchema](brBfLoc, date, source = Some("geronimo"))
    val clicks = loadParquetData[ClickTrackerRecord](ClickTrackerDataset.CLICKSS3, date)
    val performanceModelValues = loadParquetData[AdGroupPerformanceModelValueRecord](AdGroupPerformanceModelValueDataset.AGPMVS3, date);

    val modelFeatures = loadModelFeatures(featuresJson)

    if (filterResults) {
      var filterByData : DataFrame = null;

      if (fileExists(filterFilePath)(spark)) {
        filterByData = spark.read.format("csv")
          .load(filterFilePath)
          // single column is unnamed
          .withColumn(filterBy, $"_c0")
          .select(filterBy)
      } else {
        throw new Exception(f"Filter file does not exist at ${filterFilePath}")
      }

      val (filteredData, labelCounts) = filterBy match {
        case "AdGroupId" => ModelInputTransform.transform(clicks, bidsImpressions, performanceModelValues, Some(filterByData.as[AdGroupFilterRecord]), None, true, modelFeatures)
        case "Country" => ModelInputTransform.transform(clicks, bidsImpressions, performanceModelValues, None, Some(filterByData.as[CountryFilterRecord]), true, modelFeatures)
        case _ => throw new Exception(f"Cannot filter by property ${filterBy}")
      }
      //write to csv to dev for production job
      if (writeEnv != "dev") {
        writeData(filteredData, outputPath, "dev", outputPrefix, date, filteredPartitions, false)
        writeData(labelCounts, outputPath, "dev", outputPrefix + "metadata", date, 1, false)
      }
      writeData(filteredData, outputPath, writeEnv, outputPrefix, date, filteredPartitions)
      writeData(labelCounts, outputPath, writeEnv, outputPrefix + "metadata", date, 1, false)
    }

    if (!filterResults) {
      val (trainingData, labelCounts) = ModelInputTransform.transform(clicks, bidsImpressions,performanceModelValues, None, None, false, modelFeatures)
      //write to csv to dev for production job
      if (writeEnv != "dev") {
        writeData(trainingData, outputPath, "dev", outputPrefix, date, partitions, false)
        writeData(labelCounts, outputPath, "dev", "metadata", date, 1, false)
      }
      writeData(trainingData, outputPath, writeEnv, outputPrefix, date, partitions)
      writeData(labelCounts, outputPath, writeEnv, "metadata", date, 1, false)
    }
  }
}
