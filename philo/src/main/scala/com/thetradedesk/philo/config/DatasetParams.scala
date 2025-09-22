package com.thetradedesk.philo.config

import java.time.LocalDate

/**
 * Common parameters for dataset creation.
 * This contains the shared configuration that applies to all datasets.
 * Create one instance and reuse it across all dataset factories.
 * 
 * Example usage:
 *   val params = DatasetParams(outputPath, writeEnv, date, experimentName)
 *   DataWriteTransform.writeDataForDataset(GlobalDatasetFactory, params, false, labelCounts, trainingData, cols)
 */
case class DatasetParams(
  outputPath: String,
  writeEnv: String,
  date: LocalDate,
  experimentName: String = null
)