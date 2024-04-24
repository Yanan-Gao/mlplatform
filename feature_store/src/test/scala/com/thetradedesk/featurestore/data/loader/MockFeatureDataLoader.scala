package com.thetradedesk.featurestore.data.loader

import com.thetradedesk.featurestore.configs.FeatureSourceDefinition
import com.thetradedesk.featurestore.data.metrics.UserFeatureMergeJobTelemetry
import com.thetradedesk.spark.util.prometheus.PrometheusClient
import org.apache.spark.sql.{DataFrame, SparkSession}

import java.time.LocalDateTime

class MockFeatureDataLoader(implicit telemetry: UserFeatureMergeJobTelemetry) extends FeatureDataLoader {
  private val featureSourceToDataFrame = collection.mutable.Map[String, DataFrame]()

  def registerFeatureSource(userFeatureSourceDefinition: FeatureSourceDefinition, dataFrame: DataFrame) = {
    this.featureSourceToDataFrame.update(userFeatureSourceDefinition.name, dataFrame)
  }

  def clearFeatureSource() = {
    this.featureSourceToDataFrame.clear()
  }

  override def readFeatureSourceData(dateTime: LocalDateTime, userFeatureSourceDefinition: FeatureSourceDefinition)(implicit spark: SparkSession): DataFrame = {
    this.featureSourceToDataFrame(userFeatureSourceDefinition.name)
  }
}
