package job

import com.thetradedesk.geronimo.bidsimpression.schema.{BidsImpressions, BidsImpressionsSchema}
import com.thetradedesk.geronimo.shared.{loadModelFeaturesSplit, loadParquetData}
import com.thetradedesk.philo.schema.{AdGroupPerformanceModelValueDataset, AdGroupPerformanceModelValueRecord,
  ClickTrackerDataset, ClickTrackerRecord, AdGroupDataset, AdGroupRecord,
  CreativeLandingPageRecord, CreativeLandingPageDataset
}
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

case class CountryFilterRecord(Country: String)

object ModelInput {
  val date = config.getDate("date", LocalDate.now())
  val outputPath = config.getString("outputPath", "s3://thetradedesk-mlplatform-us-east-1/features/data/philo/v=3/")
  val outputPrefix = config.getStringRequired("outputPrefix")
  val ttdEnv = config.getString("ttd.env", "dev")
  val partitions = config.getInt("partitions", 2000)
  val filterResults = config.getBoolean("filterResults", false)
  val filteredPartitions = config.getInt("filteredPartitions", 200)
  val countryFilePath = config.getString("filterFilePath", "") // previously defined as fiterFilePath, use this instead to minimize change for airflow
  val filterBy = config.getString("filterBy", "AdGroupId")
  val keptCols = config.getStringSeq("keptColumns", Seq("CampaignId", "AdGroupId", "Country")) // columns to keep for debugging purpose
  val featuresJson = config.getString("featuresJson", default="s3://thetradedesk-mlplatform-us-east-1/features/data/philo/v=1/prod/schemas/features.json")

  def main(args: Array[String]): Unit = {
    val readEnv = if (ttdEnv == "prodTest") "prod" else ttdEnv
    val writeEnv = if (ttdEnv == "prodTest") "dev" else ttdEnv

    val brBfLoc = BidsImpressions.BIDSIMPRESSIONSS3 + f"${readEnv}/bidsimpressions/"

    val bidsImpressions = loadParquetData[BidsImpressionsSchema](brBfLoc, date, source = Some("geronimo"))
    val clicks = loadParquetData[ClickTrackerRecord](ClickTrackerDataset.CLICKSS3, date)
    val roi_types = Seq(3, 4) // 3, 4 for cpc and ctr
    val performanceModelValues = loadParquetData[AdGroupPerformanceModelValueRecord](AdGroupPerformanceModelValueDataset.AGPMVS3, date)
    val adgroup = loadParquetData[AdGroupRecord](AdGroupDataset.ADGROUPS3, date)

    val modelFeaturesSplit = loadModelFeaturesSplit(featuresJson)

    val modelFeatures = modelFeaturesSplit.bidRequest ++ modelFeaturesSplit.adGroup

    if (filterResults) {
      var countryFilter : DataFrame = null;
      // currently adgroup filter are moved into the pipeline without the csv adgorup list
      // it is reflected in ROI goals set to CPC and CTR in roi_types
      // Therefore, if the coutryFilePath is not empty and it is adgroup based filtering
      // then it will select the CPC and CTR adgroups within the given countries
      // otherwise, it will be just based on country
      if (!countryFilePath.isEmpty) {
        if (fileExists(countryFilePath)(spark)) {
          countryFilter = spark.read.format("csv")
            .load(countryFilePath)
            // single column is unnamed
            .withColumn("Country", $"_c0")
            .select("Country")
        } else {
          throw new Exception(f"Country filter file does not exist at ${countryFilePath}")
        }
      }
      val (filteredData, labelCounts) = filterBy match {
        // should switch to campaign if we are not doing filtering on the adgroup level
        case "AdGroupId" => ModelInputTransform.transform(
          clicks, adgroup, bidsImpressions, performanceModelValues,
          // adgroupid filtering is essentially use country and adgroup roi type to find a list of adgroups for namer
          // therefore, if the roi_types are passed in here for filtering, we don't need a separate csv file
          Some(roi_types),
          None,
          Some(countryFilter.as[CountryFilterRecord]), true, keptCols, modelFeatures)
        case "Country" => ModelInputTransform.transform(clicks, adgroup, bidsImpressions, performanceModelValues, None, None,
          Some(countryFilter.as[CountryFilterRecord]), true, keptCols, modelFeatures)
        case "LandingPage" => ModelInputTransform.transform(
          clicks, adgroup, bidsImpressions, performanceModelValues,
          // if change to campaign, this part need to be revised
          Some(roi_types),
          // TODO: add actual landing page loading code, then it will have to use the given day
          Some(spark.read.format("csv").option("header", true).load(CreativeLandingPageDataset.CREATIVELANDINGPAGES3).as[CreativeLandingPageRecord]),
          None, true, keptCols, modelFeatures
        )
        case _ => throw new Exception(f"Cannot filter by property ${filterBy}")
      }

      writeData(filteredData, outputPath, writeEnv, outputPrefix, date, filteredPartitions, false)
      writeData(labelCounts, outputPath, writeEnv, outputPrefix + "metadata", date, 1, false)
    }

    if (!filterResults) {
      val (trainingData, labelCounts) = ModelInputTransform.transform(clicks, adgroup, bidsImpressions, performanceModelValues, None, None, None, false, keptCols, modelFeatures)
      //write to csv to dev for production job
      writeData(trainingData, outputPath, writeEnv, outputPrefix, date, partitions, false)
      writeData(labelCounts, outputPath, writeEnv, "metadata", date, 1, false)
    }
  }
}
