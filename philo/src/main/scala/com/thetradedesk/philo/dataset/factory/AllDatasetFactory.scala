package com.thetradedesk.philo.dataset.factory
import com.thetradedesk.spark.util.TTDConfig.config
import org.apache.spark.sql.functions.lit

/**
 * Factory for All dataset configurations (includes all data, no business logic filtering)
 */
object AllDatasetFactory extends DatasetFactory {
  override val dataName = "global"
  override val dataPartitions = config.getInt(s"write.${dataName}.partition", 10)
  override val excludedPartitions = config.getInt(s"write.${dataName}excluded.partition", 1)
  override val baseFilter = lit(true) // Include all data
  override val isRestricted = None
}