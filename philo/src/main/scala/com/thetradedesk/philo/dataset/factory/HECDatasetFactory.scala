package com.thetradedesk.philo.dataset.factory

import com.thetradedesk.spark.util.TTDConfig.config
import org.apache.spark.sql.functions.col

/**
 * Factory for HEC dataset configurations (CategoryPolicy = "HEC")
 */
object HECDatasetFactory extends DatasetFactory {
  override val dataName = "HEC"
  override val dataPartitions = config.getInt(s"write.${dataName}.partition", 30)
  override val excludedPartitions = config.getInt(s"write.${dataName}excluded.partition", 10)
  override val baseFilter = col("CategoryPolicy") === dataName  // More DRY: uses dataName variable
  override val isRestricted = Some(1)
}