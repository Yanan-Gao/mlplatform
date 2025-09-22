package job

import com.thetradedesk.geronimo.bidsimpression.schema.{BidsImpressions, BidsImpressionsSchema}
import com.thetradedesk.geronimo.shared._
import com.thetradedesk.geronimo.shared.schemas.ModelFeature
import com.thetradedesk.philo.config.{AuxiliaryDatasetConfig, DatasetConfig, DatasetParams}
import com.thetradedesk.philo.schema.{AdGroupRecord, ClickTrackerDataSet, ClickTrackerRecord, CountryFilterRecord, CreativeLandingPageDataSet, CreativeLandingPageRecord, PartnerExclusionList, PartnerExclusionRecord, SensitiveAdvertiserDataset, SensitiveAdvertiserRecord, UnifiedAdGroupDataSet}
import com.thetradedesk.philo.{PolicyTableColumns, RoiTypes, SensitiveAdvertiserColumns}
import com.thetradedesk.philo.transform.{DataWriteTransform, ModelInputTransform}
import com.thetradedesk.spark.util.TTDConfig.config
import com.thetradedesk.spark.TTDSparkContext.spark.implicits._
import com.thetradedesk.spark.TTDSparkContext.spark
import com.thetradedesk.philo.util.FeatureUtils.{addOriginalNames, aliasedModelFeatureNames, optionalFeature}
import com.thetradedesk.philo.util.FilterUtils.{getAdGroupFilter, getCountryFilter}
import com.thetradedesk.philo.dataset.factory.{AllDatasetFactory, DatasetFactory, GlobalDatasetFactory, HECDatasetFactory, HealthDatasetFactory}
import org.apache.spark.sql.{DataFrame, Dataset}
import org.apache.spark.sql.functions.{col, lit}

import java.time.LocalDate

object ModelInput {
  // Core config values
  val date = config.getDate("date", LocalDate.now())
  val outputPath = config.getStringRequired("outputPath")
  val ttdEnv = config.getString("ttd.env", "dev")
  val writeFullData = config.getBoolean("write.data", true)
  val experimentName = config.getString("experiment", null)
  val outputFormat = config.getString("write.outputFormat", "csv") // Options: "csv" or "parquet"
  

  // Feature and filtering config
  val landingPage = config.getBoolean("landingPage", false)
  val roiFilter = config.getBoolean("roiFilter", false)
  val advertiserFilter = config.getBoolean("advertiserFilter", false)
  val countryFilePath = config.getStringOption("filterFilePath")
  val separateSensitiveAdvertisers = config.getBoolean("separateSensitiveAdvertisers", false)
  val filterClickBots = config.getBoolean("filterClickBots", false)
  val addUserData = config.getBoolean("addUserData", false)
  val debug = config.getBoolean("debug", false)
  
  
  // Data processing config  
  val keptCols = config.getStringSeq("keptColumns", Seq("AdGroupId", "Country"))
  val featuresJson = config.getStringRequired("featuresJson")
  // Moved to package object as PolicyTableColumns
  
  // Dynamic columns based on config
  var addCols = Seq("label", "excluded")
  if (separateSensitiveAdvertisers) {
    addCols = addCols ++ SensitiveAdvertiserColumns
  }
  if (debug) {
    println(s"config $config")
  }
  
  // Debug output for configuration
  println(s"Output format configuration: outputFormat = '$outputFormat'")

  /**
   * Load core input datasets (bids impressions, clicks, adgroups)
   */
  private def loadInputData(readEnv: String): (Dataset[BidsImpressionsSchema], Dataset[ClickTrackerRecord], Dataset[AdGroupRecord]) = {
    val brBfLoc = BidsImpressions.BIDSIMPRESSIONSS3 + f"${readEnv}/bidsimpressions/"
    val bidsImpressions = loadParquetData[BidsImpressionsSchema](brBfLoc, date, source = Some("geronimo"))
    val clicks = loadParquetData[ClickTrackerRecord](ClickTrackerDataSet.CLICKSS3, date)
    
    val adgroup = UnifiedAdGroupDataSet().readLatestPartitionUpTo(date, isInclusive = true)
                                        .select($"AdGroupId",$"AudienceId",$"IndustryCategoryId",
                                          $"ROIGoalTypeId",$"CampaignId").as[AdGroupRecord]
                                        .transform(
                                          ds => if (roiFilter) {getAdGroupFilter(date, ds, RoiTypes)} else ds
                                        )
    
    (bidsImpressions, clicks, adgroup)
  }

  /**
   * Parse model features from JSON configuration
   */
  private def parseModelFeatures(): (Array[ModelFeature], Array[ModelFeature], Array[ModelFeature], Seq[String]) = {
    val parsedJson = readModelFeatures(featuresJson)
    val modelFeaturesSplit = parseModelFeaturesSplitFromJson(parsedJson)

    // flagFields are special fields that are used for flagging certain options in the model
    val flagFields: Array[ModelFeature] = modelFeaturesSplit.adGroup.toArray.filter(feature =>
      optionalFeature.values.toArray.contains(feature.subtower.getOrElse("None")) )

    val bidRequestFeatures: Array[ModelFeature] = modelFeaturesSplit.bidRequest.toArray ++ flagFields
    val adGroupFeatures: Array[ModelFeature] = modelFeaturesSplit.adGroup.toArray.filter(feature =>
      !optionalFeature.values.toArray.contains(feature.subtower.getOrElse("None")))

    val seqHashFields: Array[ModelFeature] = (modelFeaturesSplit.bidRequest ++ modelFeaturesSplit.adGroup)
      .foldLeft(Array.empty[ModelFeature]) { (acc, feature) =>
        feature match {
          case ModelFeature(_, ARRAY_LONG_FEATURE_TYPE, _, _, _, _) => acc :+ feature
          case _ => acc
        }
      }
    
    val finalCols = bidRequestFeatures++adGroupFeatures
    val finalColNames = aliasedModelFeatureNames(finalCols) ++ addCols ++ addOriginalNames(keptCols)

    (bidRequestFeatures, adGroupFeatures, seqHashFields, finalColNames)
  }

  /**
   * Load optional datasets based on configuration flags
   */
  private def loadOptionalData(): (Option[Dataset[CreativeLandingPageRecord]], Option[Dataset[CountryFilterRecord]], Option[Dataset[PartnerExclusionRecord]], Option[Dataset[SensitiveAdvertiserRecord]]) = {
    val creativeLandingPage = if (landingPage) {
      Some(CreativeLandingPageDataSet().readLatestPartitionUpTo(date, isInclusive = true))
    } else None
    
    val countryFilter = countryFilePath.map(getCountryFilter)
    
    val partnerExclusionList = if (!advertiserFilter) {
      None
    } else {
      val exclusionDF = spark.read.format("csv")
                             .option("header", false)
                             .load(PartnerExclusionList.PARTNEREXCLUSIONS3)
                             .withColumnRenamed("_c0", "PartnerId")
      // if nothing is in the list, assign None to the list so that file save for exclusion list
      // won't get triggered
      if (exclusionDF.isEmpty) None
      else Some(exclusionDF.as[PartnerExclusionRecord])
    }

    val sensitiveAdvertiserData = if (separateSensitiveAdvertisers) {
      Some(SensitiveAdvertiserDataset().readLatestPartitionUpTo(date, isInclusive = true))
    } else None

    (creativeLandingPage, countryFilter, partnerExclusionList, sensitiveAdvertiserData)
  }



  /**
   * Write all output data (adgroup table, excluded advertisers, dataset configs)
   * 
   * Performance Note: trainingData should be cached before calling this method.
   * trainingData is used for adgroup table and to create preparedData as needed (select cost is minimal).
   */
  private def writeOutputData(
    trainingData: DataFrame,
    labelCounts: DataFrame,
    finalColNames: Seq[String],
    writeEnv: String,
    partnerExclusionList: Option[Dataset[PartnerExclusionRecord]],
    dataFormat: String = "csv" // "csv" or "parquet"
  ): Unit = {
    // Create adgroup table for id matching
    val adGroupTableCols = if (separateSensitiveAdvertisers) {
      Seq("originalAdGroupId", "AdGroupId", "IsRestricted", "originalAdvertiserId")
    } else {
      Seq("originalAdGroupId", "AdGroupId", "originalAdvertiserId")
    }
    
    // Create common parameters for all datasets (auxiliary and main)
    val params = DatasetParams(outputPath, writeEnv, date, experimentName)
    
    // Select final columns once for all main datasets (select cost is minimal)
    val preparedData = trainingData.select(finalColNames.map(col): _*)
    
    // Write adgroup table for id matching  
    val adGroupConfig = AuxiliaryDatasetConfig.adGroupTable(params)
    println(s"Writing adgroup table with format: '$dataFormat' to: ${adGroupConfig.getWritePath}")
    DataWriteTransform.writeDataToPath(trainingData.select(adGroupTableCols.map(col): _*).distinct(), adGroupConfig, dataFormat)
    
    // Write excluded advertisers list (once, if partner exclusion is enabled)
    if (partnerExclusionList.isDefined) {
      val excluded_data = preparedData.filter(col("excluded") === 1).drop("excluded")
      val excludedAdvertisersConfig = AuxiliaryDatasetConfig.excludedAdvertisers(params)
      DataWriteTransform.writeDataToPath(excluded_data.select("AdvertiserId").distinct(), excludedAdvertisersConfig, dataFormat)
    }
    
    // Process each dataset configuration using the new factory pattern

    // Determine which factories to use based on configuration
    val factoriesToProcess: List[DatasetFactory] = if (separateSensitiveAdvertisers) {
      List(GlobalDatasetFactory, HECDatasetFactory, HealthDatasetFactory)
    } else {
      List(AllDatasetFactory)
    }
    
    // Use the prepared data passed from main() (avoids duplicate column selection)
    
    // Process all factories sequentially - optimal for small numbers of tasks
    println(s"Processing ${factoriesToProcess.length} factories sequentially")
    
    factoriesToProcess.foreach { factory =>
      println(s"Processing factory: ${factory.getClass.getSimpleName}")
      
      // Write regular dataset (non-excluded)
      println(s"Writing main training data for ${factory.getClass.getSimpleName} with format: '$dataFormat'")
      DataWriteTransform.writeDataForDataset(factory, params, false, labelCounts, preparedData, partnerExclusionList, dataFormat)
      
      // Write excluded dataset (only if advertiser filtering is enabled)
      if (advertiserFilter) {
        DataWriteTransform.writeDataForDataset(factory, params, true, labelCounts, preparedData, partnerExclusionList, dataFormat)
      }
    }
    
    println(s"Completed processing of ${factoriesToProcess.size} factories")
  }

  def main(args: Array[String]): Unit = {
    val readEnv = if (ttdEnv == "prodTest") "prod" else ttdEnv
    val writeEnv = if (ttdEnv == "prodTest") "test" else ttdEnv

    // Load input data
    val (bidsImpressions, clicks, adgroup) = loadInputData(readEnv)
    
    // Parse model features
    val (bidRequestFeatures, adGroupFeatures, seqHashFields, finalColNames) = parseModelFeatures()
    
    // Load optional data
    val (creativeLandingPage, countryFilter, partnerExclusionList, sensitiveAdvertiserData) = loadOptionalData()
    
    // Transform data - returns cached DataFrame ready for processing
    val trainingData = ModelInputTransform.transform(
      clicks, adgroup, bidsImpressions, roiFilter,
      creativeLandingPage, countryFilter, keptCols ++ PolicyTableColumns, bidRequestFeatures, adGroupFeatures, seqHashFields, addUserData, filterClickBots,
      partnerExclusionList, addCols, sensitiveAdvertiserData, debug
    )
    
    // Generate label counts - this triggers cache materialization
    val labelCounts = ModelInputTransform.countLabels(trainingData, sensitiveAdvertiserData)
    println("Training data: cached and materialized by label count computation")
    
    try {
      // Write output data - select operations happen as needed (minimal cost)
      println(s"About to write output data with format: '$outputFormat'")
      writeOutputData(trainingData, labelCounts, finalColNames, writeEnv, partnerExclusionList, outputFormat)
    } finally {
      // Clean up cache to free memory
      println("Cleaning up cached training data...")
      trainingData.unpersist(blocking = false)
    }
  }
}
