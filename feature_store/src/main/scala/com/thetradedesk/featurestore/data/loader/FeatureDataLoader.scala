package com.thetradedesk.featurestore.data.loader

import com.thetradedesk.featurestore.configs.FeatureSourceDefinition
import com.thetradedesk.featurestore.data.metrics.UserFeatureMergeJobTelemetry
import com.thetradedesk.featurestore.data.rules.DataValidationRule
import com.thetradedesk.spark.util.io.FSUtils
import com.thetradedesk.spark.util.prometheus.PrometheusClient
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.{DataFrame, SparkSession}

import java.time.{LocalDate, LocalDateTime}
import java.time.format.DateTimeFormatter

class FeatureDataLoader(implicit telemetry: UserFeatureMergeJobTelemetry) {
  /**
   * Read partition from S3 based on feature source definition
   * the columns returned could have different data type which definied in feature definition
   * TODO: support hourly data reading
   *
   * @param dateTime
   * @param userFeatureSourceDefinition
   * @param spark
   * @return columns in the feature definition and IdKey only
   */
  def readFeatureSourceData(dateTime: LocalDateTime,
                            userFeatureSourceDefinition: FeatureSourceDefinition)(implicit spark: SparkSession): DataFrame = {
    var paths = (0 to userFeatureSourceDefinition.lookBack).map(x => userFeatureSourceDefinition.basePath(if (userFeatureSourceDefinition.lookBackOnDay) dateTime.minusDays(x) else dateTime.minusHours(x)))

    if (!validatePaths(userFeatureSourceDefinition.name, userFeatureSourceDefinition.dataValidationRule, paths)) {
      throw new RuntimeException(s"[Features Data Loading] ${userFeatureSourceDefinition.name} data is missing")
    }

    paths = paths.filter(FSUtils.directoryExists(_))
    if (paths.isEmpty) {
      throw new RuntimeException(s"user feature source ${userFeatureSourceDefinition.name} not exists")
    }

    val df = userFeatureSourceDefinition.format match {
      case "tfrecord" => spark
        .read
        .format("tfrecord")
        .option("recordType", "Example")
        .load(paths: _*)
      case "tsv" => spark
        .read
        .format("com.databricks.spark.csv")
        .option("delimiter", "\t")
        .option("header", "true")
        .option("inferSchema", "true")
        .load(paths.flatMap(x => FSUtils.listFiles(x, true)(spark).map(y => x + "/" + y)): _*)
      case "parquet" => spark
        .read
        .parquet(paths: _*)
      case _ => throw new UnsupportedOperationException(s"format ${userFeatureSourceDefinition.format} is not supported")
    }

    df.select(userFeatureSourceDefinition.features.map(x => col(x.name)) :+ col(userFeatureSourceDefinition.idKey): _*)
  }

  private def validatePaths(dataSourceName: String, dataValidationRule: Option[DataValidationRule], paths: Traversable[String])(implicit sparkSession: SparkSession): Boolean = {
    if (paths.isEmpty) {
      telemetry.dataLoadingCount.labels(dataSourceName, "validation_no_path")
      return false
    }
    dataValidationRule match {
      case Some(DataValidationRule.AtLeastOne) if paths.forall(x => !FSUtils.directoryExists(x)) =>
        telemetry.dataLoadingCount.labels(dataSourceName, "validation_all_missing")
        false
      case Some(DataValidationRule.AllExist) if paths.exists(x => !FSUtils.directoryExists(x)) =>
        telemetry.dataLoadingCount.labels(dataSourceName, "validation_some_missing")
        false
      case _ =>
        telemetry.dataLoadingCount.labels(dataSourceName, "validation_success")
        true
    }
  }
}