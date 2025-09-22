package com.thetradedesk.philo.dataset.factory

import com.thetradedesk.philo.config.{DatasetConfig, DatasetParams}
import org.apache.spark.sql.Column
import org.apache.spark.sql.functions.col

/**
 * Base factory trait for dataset configurations.
 * Defines the common pattern that all dataset factories follow.
 */
trait DatasetFactory {
  // Each factory defines its own configuration
  def dataPartitions: Int
  def excludedPartitions: Int
  def baseFilter: Column
  def isRestricted: Option[Int]
  
  // Base names for different types
  def dataName: String
  def metadataName: String = dataName + "metadata"
  def excludedDataName: String = dataName + "excluded"
  def excludedMetadataName: String = excludedDataName + "metadata"
  def lineCountName: String = dataName + "linecounts"
  def excludedLineCountName: String = excludedDataName + "linecounts"
  
  // Factory methods that all datasets support
  def data(params: DatasetParams): DatasetConfig = {
    DatasetConfig(
      name = dataName,
      partitions = dataPartitions,
      filterCondition = baseFilter && col("excluded") === 0,
      outputPath = params.outputPath,
      writeEnv = params.writeEnv,
      date = params.date,
      experimentName = params.experimentName,
      isRestricted = isRestricted
    )
  }
  
  def metadata(params: DatasetParams): DatasetConfig = {
    DatasetConfig(
      name = metadataName,
      partitions = 1,
      filterCondition = baseFilter && col("excluded") === 0,
      outputPath = params.outputPath,
      writeEnv = params.writeEnv,
      date = params.date,
      experimentName = params.experimentName,
      isRestricted = isRestricted
    )
  }
  
  def excluded(params: DatasetParams): DatasetConfig = {
    DatasetConfig(
      name = excludedDataName,
      partitions = excludedPartitions,
      filterCondition = baseFilter && col("excluded") === 1,
      outputPath = params.outputPath,
      writeEnv = params.writeEnv,
      date = params.date,
      experimentName = params.experimentName,
      isRestricted = isRestricted
    )
  }
  
  def excludedMetadata(params: DatasetParams): DatasetConfig = {
    DatasetConfig(
      name = excludedMetadataName,
      partitions = 1,
      filterCondition = baseFilter && col("excluded") === 1,
      outputPath = params.outputPath,
      writeEnv = params.writeEnv,
      date = params.date,
      experimentName = params.experimentName,
      isRestricted = isRestricted
    )
  }
  
  def lineCounter(params: DatasetParams): DatasetConfig = {
    DatasetConfig(
      name = lineCountName,
      partitions = 1,
      filterCondition = baseFilter && col("excluded") === 0,
      outputPath = params.outputPath,
      writeEnv = params.writeEnv,
      date = params.date,
      experimentName = params.experimentName,
      isRestricted = isRestricted
    )
  }
  
  def excludedLineCounter(params: DatasetParams): DatasetConfig = {
    DatasetConfig(
      name = excludedLineCountName,
      partitions = 1,
      filterCondition = baseFilter && col("excluded") === 1,
      outputPath = params.outputPath,
      writeEnv = params.writeEnv,
      date = params.date,
      experimentName = params.experimentName,
      isRestricted = isRestricted
    )
  }
}