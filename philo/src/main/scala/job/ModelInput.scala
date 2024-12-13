package job

import com.thetradedesk.geronimo.bidsimpression.schema.{BidsImpressions, BidsImpressionsSchema}
import com.thetradedesk.geronimo.shared.{loadModelFeatures, loadParquetData, parseModelFeaturesSplitFromJson}
import com.thetradedesk.philo.schema.{AdGroupRecord, AdvertiserExclusionList, AdvertiserExclusionRecord, CampaignROIGoalDataSet, CampaignROIGoalRecord, ClickTrackerDataSet, ClickTrackerRecord, CreativeLandingPageDataSet, CreativeLandingPageRecord, UnifiedAdGroupDataSet}
import com.thetradedesk.philo.transform.ModelInputTransform
import com.thetradedesk.spark.util.TTDConfig.config
import com.thetradedesk.spark.TTDSparkContext.spark.implicits._
import com.thetradedesk.spark.TTDSparkContext.spark
import com.thetradedesk.philo.{writeData, countLinePerFile}
import com.thetradedesk.spark.TTDSparkContext
import com.thetradedesk.spark.util.io.FSUtils
import com.thetradedesk.spark.util.io.FSUtils.fileExists
import org.apache.spark.sql.{DataFrame, Dataset}
import org.apache.spark.sql.functions.{coalesce, lit}

import scala.io.Source

import java.time.LocalDate

case class CountryFilterRecord(Country: String)

object ModelInput {
  val date = config.getDate("date", LocalDate.now())
  val outputPath = config.getStringRequired("outputPath")
  val outputPrefix = config.getStringRequired("outputPrefix")
  val ttdEnv = config.getString("ttd.env", "dev")
  val partitions = config.getInt("partitions", 200)
  val writeFullData = config.getBoolean("write.data", true)
  val writeMeta = config.getBoolean("write.meta", true)
  val writePerFile = config.getBoolean("write.perfile", true)
  // if landingPage is true, will read landing page and merge the info into main dataset
  // if roiFilter is true, will use campaign roi goal to filter the adgroups
  // if countryFilePath is provided, will add the country filter
  // for the current process, for apac and emea, only country file will be provided, and for namer, roiFilter will be
  // set to true, country file will also be provided
  // in the future, if we are using landingpage, landingPage will be set to true and roiFilter will be set to true
  // if we only have one region, country filter will be removed
  val landingPage = config.getBoolean("landingPage", false)
  val roiFilter = config.getBoolean("roiFilter", false)
  val advertiserFilter =  config.getBoolean("advertiserFilter", false)
  val countryFilePath = config.getStringOption("filterFilePath") // previously defined as fiterFilePath, use this instead to minimize change for airflow
  val keptCols = config.getStringSeq("keptColumns", Seq("AdGroupId", "Country")) // columns to keep for debugging purpose
  val featuresJson = config.getStringRequired("featuresJson")
  val debug = config.getBoolean("debug", false)

  // Attributes used for adding user data and for click-bot filtering
  val filterClickBots = config.getBoolean("filterClickBots", false)
  val addUserData = config.getBoolean("addUserData", false)
  val numUserCols = config.getInt("numUserCols", 170)

  if (debug) {
    println(s"config $config")
  }
  def getAdGroupFilter(date: LocalDate, adgroup: Dataset[AdGroupRecord],
                       roi_types: Seq[Int]): Dataset[AdGroupRecord] = {
    val campaignGoal = CampaignROIGoalDataSet().readLatestPartitionUpTo(date, isInclusive = true)
      // get primary goal only
      .filter($"Priority" === 1)
    adgroup.as("ag").join(campaignGoal.as("cg"), Seq("CampaignId"), "left")
      // consider campaign goal first, fall back to adgroup goal if not defined
      .withColumn("goal", coalesce($"cg.ROIGoalTypeId", $"ag.ROIGoalTypeId"))
      .filter($"goal".isin(roi_types: _*))
      .select("ag.*")
      .as[AdGroupRecord]
  }

  def getCountryFilter(countryFilePath: String): Dataset[CountryFilterRecord] = {
    // currently adgroup filter are moved into the pipeline without the csv adgorup list
    // it is reflected in ROI goals set to CPC and CTR in roi_types
    // Therefore, if the coutryFilePath is not empty and it is adgroup based filtering
    // then it will select the CPC and CTR adgroups within the given countries
    // otherwise, it will be just based on country
    if (!countryFilePath.isEmpty) {
      if (fileExists(countryFilePath)(spark)) {
        spark.read.format("csv")
          .load(countryFilePath)
          // single column is unnamed
          .withColumn("Country", $"_c0")
          .select("Country").as[CountryFilterRecord]
      } else {
        throw new Exception(f"Country filter file does not exist at ${countryFilePath}")
      }
    } else {
      throw new Exception(f"Country file path is empty")
    }
  }

  def readModelFeatures(srcPath: String)(): String = {
    var rawJson: String = null
    if (FSUtils.isLocalPath(srcPath)(spark)) {
      rawJson = Option(getClass.getResourceAsStream(srcPath))
        .map { inputStream =>
          try {
            Source.fromInputStream(inputStream).getLines.mkString("\n")
          } finally {
            inputStream.close()
          }
        }.getOrElse(throw new IllegalArgumentException(s"Resource not found in JAR: $srcPath"))
    } else {
      rawJson = FSUtils.readStringFromFile(srcPath)(spark)
    }
    rawJson
  }


  def main(args: Array[String]): Unit = {
    val readEnv = if (ttdEnv == "prodTest") "prod" else ttdEnv
    val writeEnv = if (ttdEnv == "prodTest") "dev" else ttdEnv

    val brBfLoc = BidsImpressions.BIDSIMPRESSIONSS3 + f"${readEnv}/bidsimpressions/"

    val bidsImpressions = loadParquetData[BidsImpressionsSchema](brBfLoc, date, source = Some("geronimo"))
    val clicks = loadParquetData[ClickTrackerRecord](ClickTrackerDataSet.CLICKSS3, date)
    val roi_types = Seq(3, 4) // 3, 4 for cpc and ctr
    // if roiFilter, filter use the adgroup as a filter, else just left join to get the adgroup info
    val adgroup = UnifiedAdGroupDataSet().readLatestPartitionUpTo(date, isInclusive = true)
                                         .select($"AdGroupId",$"AudienceId",$"IndustryCategoryId",
                                           $"ROIGoalTypeId",$"CampaignId").as[AdGroupRecord]
                                         .transform(
                                           ds => if (roiFilter) {getAdGroupFilter(date, ds, roi_types)} else ds
                                         )

    val parsedJson = readModelFeatures(featuresJson)
    val modelFeaturesSplit = parseModelFeaturesSplitFromJson(parsedJson)
    val creativeLandingPage = if (landingPage) {
      Some(CreativeLandingPageDataSet().readLatestPartitionUpTo(date, isInclusive = true))
    } else None
    val countryFilter = countryFilePath.map(getCountryFilter)
    val advertiserExclusionList = if (outputPrefix == "processed" || !advertiserFilter) {
                                    None
                                  } else {
                                    val exclusionDF = spark.read.format("csv")
                                                           .option("header", false)
                                                           .load(AdvertiserExclusionList.ADVERTISEREXCLUSIONS3)
                                                           .withColumnRenamed("_c0", "AdvertiserId")
                                    // if nothing is in the list, assign None to the list so that file save for exclusion list
                                    // won't get triggered
                                    if (exclusionDF.isEmpty) None
                                    else Some(exclusionDF.as[AdvertiserExclusionRecord])}


    val modelFeatures = modelFeaturesSplit.bidRequest ++ modelFeaturesSplit.adGroup
    val (trainingData, labelCounts) = ModelInputTransform.transform(
      clicks, adgroup, bidsImpressions, roiFilter,
      creativeLandingPage, countryFilter, keptCols, modelFeatures, addUserData, filterClickBots, numUserCols,
      advertiserExclusionList, debug
    )
    //TODO if non exclusion list, set all exclusion flag to 0
    if (writeFullData) {
      //write to csv to dev for production job
      writeData(
        trainingData.filter($"excluded"===0).drop("excluded"),
        outputPath, writeEnv, outputPrefix, date, partitions, false
      )
      if (advertiserExclusionList.isDefined) {
        val excluded_data = trainingData.filter($"excluded"===1).drop("excluded").cache()
        writeData(excluded_data, outputPath, writeEnv, outputPrefix+"excluded", date, 20, false)
        writeData(excluded_data.select("AdvertiserId").distinct(), outputPath, writeEnv, outputPrefix+"excludedadvertisers",
          date, 1, false)
        excluded_data.unpersist()
      }
    }
    if (writePerFile) {
      val linesPerFileName = if (outputPrefix == "processed") {
        "linecounts"
      } else {
        outputPrefix + "linecounts"
      }
      val lineCountsPerFileNonEx = countLinePerFile(outputPath, writeEnv, outputPrefix, date)(spark)
      writeData(lineCountsPerFileNonEx, outputPath, writeEnv, linesPerFileName, date, 1, false)
      if (advertiserExclusionList.isDefined) {
        val lineCountsPerFileEx = countLinePerFile(outputPath, writeEnv, outputPrefix+"excluded", date)(spark)
        writeData(lineCountsPerFileEx, outputPath, writeEnv, linesPerFileName+"excluded", date, 1, false)
      }
    }
    if (writeMeta) {
      val metadataName = if (outputPrefix == "processed") {
        "metadata"
      } else {
        outputPrefix + "metadata"
      }
      writeData(labelCounts.filter($"excluded"===0).drop("excluded"), outputPath, writeEnv, metadataName, date, 1, false)
      if (advertiserExclusionList.isDefined) {
        writeData(labelCounts.filter($"excluded" === 1).drop("excluded"), outputPath, writeEnv, metadataName + "excluded", date, 1, false)
      }
    }
    writeData(trainingData.select("originalAdGroupId", "AdGroupId").distinct(), outputPath, writeEnv, "adgrouptable", date, 1, false)


  }
}
