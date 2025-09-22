package com.thetradedesk.philo.config

import com.thetradedesk.geronimo.shared.explicitDatePart
import org.apache.spark.sql.{Column, functions}
import java.time.LocalDate

/**
 * Simple case class for dataset configurations.
 * Contains only essential properties - factories handle the complexity.
 */
case class DatasetConfig(
  name: String,
  partitions: Int,
  filterCondition: Column,
  outputPath: String,
  writeEnv: String,
  date: LocalDate,
  experimentName: String = null,
  isRestricted: Option[Int] = None
) {
  def getWritePath: String = {
    if (experimentName == null) {
      s"$outputPath/$writeEnv/$name/${explicitDatePart(date)}"
    } else {
      s"$outputPath/$writeEnv/experiment=$experimentName/$name/${explicitDatePart(date)}"
    }
  }
}

/**
 * Factory for creating auxiliary dataset configurations.
 * These are helper datasets like adgroup tables, exclusion lists, etc.
 */
object AuxiliaryDatasetConfig {
  
  /**
   * Create configuration for adgroup table (used for ID matching).
   * Standard auxiliary dataset: 1 partition, no filtering, no restrictions.
   */
  def adGroupTable(params: DatasetParams): DatasetConfig = {
    DatasetConfig(
      name = "adgrouptable",
      partitions = 1,
      filterCondition = functions.lit(true),
      outputPath = params.outputPath,
      writeEnv = params.writeEnv,
      date = params.date,
      experimentName = params.experimentName,
      isRestricted = None
    )
  }
  
  /**
   * Create configuration for excluded advertisers list.
   * Standard auxiliary dataset: 1 partition, no filtering, no restrictions.
   */
  def excludedAdvertisers(params: DatasetParams): DatasetConfig = {
    DatasetConfig(
      name = "globalexcludedadvertisers",
      partitions = 1,
      filterCondition = functions.lit(true),
      outputPath = params.outputPath,
      writeEnv = params.writeEnv,
      date = params.date,
      experimentName = params.experimentName,
      isRestricted = None
    )
  }
}