package com.thetradedesk.philo.transform

import com.thetradedesk.philo.config.{DatasetConfig, DatasetParams}
import com.thetradedesk.philo.dataset.factory.DatasetFactory
import com.thetradedesk.philo.schema.PartnerExclusionRecord
import com.thetradedesk.philo.SensitiveAdvertiserColumns
import com.thetradedesk.spark.util.TTDConfig.config
import com.thetradedesk.spark.TTDSparkContext.spark
import job.ModelInput.ttdEnv
import org.apache.spark.sql.{DataFrame, Dataset, SaveMode, SparkSession}
import org.apache.spark.sql.functions.{col, input_file_name, udf}
import java.time.LocalDate
import com.thetradedesk.geronimo.shared.explicitDatePart

/**
 * Data writing utilities with support for different output modes.
 * 
 * Configuration options:
 * - write.meta: Enable/disable metadata writing (default: true)
 * - write.data: Enable/disable training data writing (default: true)  
 * - write.perfile: Enable/disable line count writing (default: true)
 * - write.dummyRun: Enable dummy run mode - prints paths without processing data (default: false)
 * - separateSensitiveAdvertisers: Enable separate processing for sensitive advertisers (default: false)
 * 
 * Dummy run mode usage:
 *   spark-submit --conf spark.ttd.write.dummyRun=true ...
 *   or set in application.conf: write.dummyRun = true
 *   
 * Note: Dummy run mode is FAST - it skips all data processing and aggregations,
 * only printing the S3 paths where data would be written.
 */
object DataWriteTransform {
  

  private val writeMeta = config.getBoolean("write.meta", true)
  private val writeFullData = config.getBoolean("write.data", true)
  private val writePerFile = config.getBoolean("write.perfile", true)
  private val dummyRun = config.getBoolean("write.dummyRun", false)
  private val separateSensitiveAdvertisers = config.getBoolean("separateSensitiveAdvertisers", false)

  // Sensitive advertiser columns are now defined in the package object


  /**
   * Write auxiliary data directly to a specific path (for metadata, line counts, etc.)
   * This method doesn't apply filtering or config logic - it's for simple data writes
   * In dummy run mode, it only prints the path without writing data
   */
  def writeDataToPath(df: DataFrame, dataConfig: DatasetConfig, dataFormat: String = "csv"): Unit = {
    if (dummyRun) {
      println(s"[DUMMY RUN] Would write ${dataConfig.name} (${dataConfig.partitions} partitions) to: ${dataConfig.getWritePath} (format: $dataFormat)")
    } else {
      // Use coalesce only for single partition (common for auxiliary files), otherwise repartition
      val targetPartitions = dataConfig.partitions
      val optimizedDf = if (targetPartitions == 1) {
        println(s"Coalescing to single partition for ${dataConfig.name}")
        df.coalesce(1)
      } else {
        println(s"Repartitioning to ${targetPartitions} partitions for ${dataConfig.name}")
        df.repartition(targetPartitions)
      }
      
      val writer = optimizedDf.write.mode(SaveMode.Overwrite)
      
      dataFormat.toLowerCase match {
        case "csv" =>
                    println(s"Writing CSV format to: ${dataConfig.getWritePath}")
          writer.option("header", "true").csv(dataConfig.getWritePath)
        case "parquet" =>
          println(s"Writing Parquet format to: ${dataConfig.getWritePath}")
          writer
            .option("compression", config.getString("write.parquet.compression", "lz4")) // LZ4: excellent Ray compatibility + speed
            .option("parquet.block.size", config.getString("write.parquet.blockSize", "67108864")) // 64MB blocks (better for Ray parallel reads)
            .option("parquet.page.size", config.getString("write.parquet.pageSize", "1048576")) // 1MB pages
            .option("parquet.dictionary.page.size", config.getString("write.parquet.dictionaryPageSize", "1048576")) // 1MB dictionary pages
            .option("parquet.enable.dictionary", config.getString("write.parquet.enableDictionary", "true")) // Enable dictionary encoding
            .parquet(dataConfig.getWritePath)
        case _ =>
          println(s"Warning: Unknown format '$dataFormat', defaulting to parquet")
          writer.parquet(dataConfig.getWritePath)
      }
    }
  }

  /**
   * Convenience method to write all data types using the new factory pattern
   * Example usage:
   *   val params = DatasetParams(outputPath, writeEnv, date, experimentName)
   *   val preparedData = trainingData.select(finalColNames.map(col): _*)
   *   writeDataForDataset(GlobalDatasetFactory, params, false, labelCounts, preparedData, exclusionList)
   */
  def writeDataForDataset(
    factory: DatasetFactory,
    params: DatasetParams,
    isExcluded: Boolean,
    labelCounts: DataFrame,
    preparedData: DataFrame, // Pre-selected data with final columns already applied
    partnerExclusionList: Option[Dataset[PartnerExclusionRecord]] = None,
    dataFormat: String = "csv", // "csv" or "parquet"
  ): Unit = {
    val dataConfig = if (isExcluded) factory.excluded(params) else factory.data(params)
    val metadataConfig = if (isExcluded) factory.excludedMetadata(params) else factory.metadata(params)
    val lineCountConfig = if (isExcluded) factory.excludedLineCounter(params) else factory.lineCounter(params)
    println(s"dataFormat is $dataFormat in writeDataForDataset")
    
    // Calculate metadata for this specific dataset
    val metadataData = labelCounts
      .filter(metadataConfig.filterCondition)
      .drop("excluded", "CategoryPolicy")
    
    // Use partition count directly from config - simple and predictable
    println(s"Using configured partitions for ${dataConfig.name}: ${dataConfig.partitions}")
    
    // Write metadata
    if (writeMeta) {
      if (dummyRun) {
        println(s"[DUMMY RUN] Would write metadata for ${dataConfig.name} (${metadataConfig.partitions} partitions) to: ${metadataConfig.getWritePath}")
      } else {
        writeDataToPath(metadataData, metadataConfig, dataFormat)
      }
    }

    // Write training data
    if (writeFullData) {
      if (dummyRun) {
        // Early return for excluded data without exclusion list
        if (isExcluded && !partnerExclusionList.isDefined) {
          println(s"[DUMMY RUN] Skipping excluded data for ${dataConfig.name} - no partner exclusion list provided")
          return
        }
        val excludedText = if (isExcluded) " (excluded)" else ""
        println(s"[DUMMY RUN] Would write ${dataConfig.name}${excludedText} training data (${dataConfig.partitions} partitions) to: ${dataConfig.getWritePath}")
      } else {
        // Early return for excluded data without exclusion list
        if (isExcluded && !partnerExclusionList.isDefined) return

        // Use the pre-selected data directly (no need to select again)
        var filteredData = preparedData
          .filter(dataConfig.filterCondition)
          .drop("excluded")

        // Apply isRestricted filter if specified
        dataConfig.isRestricted.foreach { restrictedValue =>
          filteredData = filteredData.filter(col("IsRestricted") === restrictedValue)
        }

        // Drop sensitive advertiser columns if enabled
        if (separateSensitiveAdvertisers) {
          filteredData = filteredData.drop(SensitiveAdvertiserColumns: _*)
        }

        writeDataToPath(filteredData, dataConfig, dataFormat)
      }
    }

    // Write line counts
    if (writePerFile) {
      if (dummyRun) {
        println(s"[DUMMY RUN] Would write line counts for ${dataConfig.name} to: ${lineCountConfig.getWritePath}")
      } else {
        val datasetName = dataConfig.name
        val lineCountData = countLinePerFile(params.outputPath, params.writeEnv, datasetName, params.date, params.experimentName)(spark)
        writeDataToPath(lineCountData, lineCountConfig, "csv") // Line counts are always CSV for readability
      }
    }
  }

  /**
   * Count lines per file in the written output
   */
  def countLinePerFile(outputPath: String, ttdEnv: String, outputPrefix: String, date: LocalDate, experimentName: String = null)(implicit spark: SparkSession): DataFrame = {
    // even though it says parquet, there's nothing parquet specific in this method
    val writePath = if (experimentName == null) {
      s"$outputPath/$ttdEnv/$outputPrefix/${explicitDatePart(date)}"
    } else {
      s"$outputPath/$ttdEnv/experiment=$experimentName/$outputPrefix/${explicitDatePart(date)}"
    }

    val df = spark.read.format("csv")
            .option("header", "true") // Adjust based on whether the files contain headers
            .csv(writePath)
    val dfWithFileName = df.withColumn("full_file_name", input_file_name())
    val extractFileName = udf((fullPath: String) => {
      fullPath.split("/").last // Split by "/" and take the last part (the file name)
    })
    val dfWithBaseFileName = dfWithFileName.withColumn("file_name", extractFileName(col("full_file_name")))
    val lineCountsPerFile = dfWithBaseFileName
                           .groupBy("file_name")
                           .count()
    lineCountsPerFile
  }
}