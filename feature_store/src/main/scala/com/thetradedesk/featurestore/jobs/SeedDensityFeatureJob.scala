package com.thetradedesk.featurestore.jobs

import com.thetradedesk.featurestore.configs.UserFeatureMergeDefinition
import com.thetradedesk.featurestore.constants.FeatureConstants
import com.thetradedesk.featurestore.data.generators.CustomBufferDataGenerator
import com.thetradedesk.featurestore.data.metrics.UserFeatureMergeJobTelemetry
import com.thetradedesk.featurestore.datasets.{SeedDensityFeature, SeedDensityFeatureDataset}
import com.thetradedesk.featurestore.utils.FileHelper
import com.thetradedesk.featurestore.{dateTime}
import com.thetradedesk.spark.util.prometheus.PrometheusClient
import com.thetradedesk.spark.TTDSparkContext.spark
import org.apache.spark.sql.functions.col
import com.thetradedesk.spark.TTDSparkContext.spark.implicits._
import com.thetradedesk.spark.util.TTDConfig.config
import org.apache.spark.sql.SaveMode
import upickle.default._

object SeedDensityFeatureJob {
  private implicit val prometheus = new PrometheusClient("FeatureStore", "SeedDensityFeatureJob")
  private implicit val telemetry = UserFeatureMergeJobTelemetry()

  object Config {
    val userFeatureMergeDefinitionPath = config.getString("seedDensityDefinitionPath", default = "/seedDensityFeatureDefinition.json")
  }

  def main(args: Array[String]): Unit = {
    val start = System.currentTimeMillis()
    try {
      runETLPipeline()
    } finally {
      telemetry.jobRunningTime.labels(dateTime.toString).set(System.currentTimeMillis() - start)
      prometheus.pushMetrics()
    }
  }

  def runETLPipeline(): Unit = {
    // step 1: collect feature definitions
    val definition = FileHelper.readStringFromFile(Config.userFeatureMergeDefinitionPath, true)(spark)
    val userFeatureMergeDefinition = read[UserFeatureMergeDefinition](definition)
    // step 2: join features, generate feature data (byte array) <multi data generator supported> and feature schema (verify data length at the same time)
    val (df, schema) = new CustomBufferDataGenerator()(spark, telemetry).generate(dateTime, userFeatureMergeDefinition)
    // step 4: verify feature data (randomly by 1%) -- maybe in a separate job with C# <even data server>
    // step 5: write data to s3 (with partitions to ensure each partition less than 100 MB)
    SeedDensityFeatureDataset(userFeatureMergeDefinition)
      .writePartition(
        df.select(col(FeatureConstants.FeatureKeyValueHashedKey), col(FeatureConstants.FeatureDataKey)).as[SeedDensityFeature]
        , dateTime
        , saveMode = SaveMode.Overwrite)
    // step 6: finish job and clean
  }
}
