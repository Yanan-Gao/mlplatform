package com.thetradedesk.philo.dataset.factory

import com.thetradedesk.spark.util.TTDConfig.config
import org.apache.spark.sql.functions.col

/**
 * Factory for Global dataset configurations (CategoryPolicy is null)
 */
object GlobalDatasetFactory extends DatasetFactory {
  override val dataName = "global"
  override val dataPartitions = config.getInt(s"write.${dataName}.partition", 200)
  override val excludedPartitions = config.getInt(s"write.${dataName}excluded.partition", 20)
  override val baseFilter = col("CategoryPolicy").isNull
  override val isRestricted = Some(0)
}