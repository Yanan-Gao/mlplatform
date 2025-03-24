package com.thetradedesk.audience.jobs

import com.thetradedesk.audience.datasets.{AdGroupDataSet, AudienceModelPolicyReadableDataset, AudienceModelThresholdRecord, AudienceModelThresholdWritableDataset, CampaignSeedDataset, Model}
import com.thetradedesk.audience.jobs.AudienceCalibrationAndMergeJob.prometheus
import com.thetradedesk.audience.{date, dateTime, ttdReadEnv, ttdWriteEnv, audienceVersionDateFormat}
import com.thetradedesk.audience.utils.S3Utils
import com.thetradedesk.spark.TTDSparkContext.spark
import com.thetradedesk.spark.TTDSparkContext.spark.implicits._
import com.thetradedesk.spark.util.TTDConfig.config
import com.thetradedesk.spark.util.prometheus.PrometheusClient
import org.apache.spark.sql.SaveMode
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.FloatType
import java.time.{LocalDate, LocalDateTime}
import java.time.format.DateTimeFormatter
import org.apache.spark.sql.DataFrame

object AudienceCalibrationAndMergeJob {
  val prometheus = new PrometheusClient("AudienceModelJob", "AudienceCalibrationAndMergeJob")

  object Config {
    val model = Model.withName(config.getString("modelName", default = "RSM"))
    val mlplatformS3Bucket = S3Utils.refinePath(config.getString("mlplatformS3Bucket", "thetradedesk-mlplatform-us-east-1"))
    val tmpEmbeddingDataS3Path = S3Utils.refinePath(config.getString("tmpEmbeddingDataS3Path", s"configdata/${ttdReadEnv}/audience/embedding_temp/RSMV2/v=1"))
    val embeddingDataS3Path = S3Utils.refinePath(config.getString("embeddingDataS3Path", s"configdata/${ttdReadEnv}/audience/embedding/RSMV2/v=1"))
    val inferenceDataS3Path = S3Utils.refinePath(config.getString("inferenceDataS3Path", s"data/${ttdWriteEnv}/audience/RSMV2/prediction"))
    val embeddingRecentVersion = config.getString("embeddingRecentVersion", default = null)
    val anchorStartDate = config.getDate("anchorStartDate",  default = LocalDate.parse("2025-03-05"))
    val smoothFactor = config.getDouble("smoothFactor", default = 30.0)
    val locationFactor = config.getDouble("locationFactor", default = 0.8)
    val baselineHitRate = config.getDouble("baselineHitRate", default = 1E-8)
    val recursiveWeight = config.getDouble("recursiveWeight", default = 0.1)
    val writeMode = config.getString("writeMode", "overwrite")
  }

  def main(args: Array[String]): Unit = {
    runETLPipeline()
    prometheus.pushMetrics()
  }

  def runETLPipeline(): Unit = {

    val formatter = DateTimeFormatter.ofPattern("yyyyMMdd")
    val dateTime = date.atStartOfDay()

    val embeddingTableDateFormatter = DateTimeFormatter.ofPattern(audienceVersionDateFormat)
    val availableTmpEmbeddingVersions = S3Utils
    .queryCurrentDataVersions(Config.mlplatformS3Bucket, Config.tmpEmbeddingDataS3Path)
    .map(LocalDateTime.parse(_, embeddingTableDateFormatter))
    .toSeq
    .sortWith(_.isAfter(_))

    val recentVersionOption = if (Config.embeddingRecentVersion != null) Some(Config.embeddingRecentVersion)
    else availableTmpEmbeddingVersions.find(_.isBefore(dateTime))

    val recentVersionStr = recentVersionOption.get.asInstanceOf[java.time.LocalDateTime].toLocalDate.format(formatter)

    val previousEmbeddingPath = s"s3://${Config.mlplatformS3Bucket}/${Config.embeddingDataS3Path}/${recentVersionStr}000000"
    val currentTmpEmbeddingPath = s"s3://${Config.mlplatformS3Bucket}/${Config.tmpEmbeddingDataS3Path}/${date.format(formatter)}000000"
    val outputEmbeddingPath = s"s3://${Config.mlplatformS3Bucket}/${Config.embeddingDataS3Path}/${date.format(formatter)}000000"
    
    val currentTmpEmbeddingTable = spark.read.parquet(currentTmpEmbeddingPath)
    val previousEmbeddingTable = spark.read.parquet(previousEmbeddingPath)
    

    val previousCalibrationPath = s"s3://${Config.mlplatformS3Bucket}/${Config.inferenceDataS3Path}/calibration_data/v=1/model_version=${recentVersionStr}000000/${date.format(formatter)}000000/"
    val currentCalibrationPath = s"s3://${Config.mlplatformS3Bucket}/${Config.inferenceDataS3Path}/calibration_data/v=1/model_version=${date.format(formatter)}000000/${date.format(formatter)}000000/"
    val populationPath = s"s3://${Config.mlplatformS3Bucket}/${Config.inferenceDataS3Path}/population_data/v=1/model_version=${date.format(formatter)}000000/${date.format(formatter)}000000/"
    
    val populationInferenceData = spark.read.parquet(populationPath)
    val calibrationCurrentModelInferenceData = spark.read.parquet(currentCalibrationPath)
    val calibrationPreviousModelInferenceData = spark.read.parquet(previousCalibrationPath)

    def explodeSyntheticIds(df: DataFrame): DataFrame = {
        df.withColumn("zipped", arrays_zip($"SyntheticIds", $"Targets", $"pred"))
          .withColumn("exploded", explode($"zipped"))
          .withColumn("SyntheticId", $"exploded.SyntheticIds")
          .withColumn("Target", $"exploded.Targets")
          .withColumn("pred", $"exploded.pred")
          .drop("zipped", "exploded")
      }


    val calibrationCurrentVersionMetric = explodeSyntheticIds(calibrationCurrentModelInferenceData)
                                            .groupBy("SyntheticId")
                                            .agg(avg("pred").alias("current_avg_pred"))

    val calibrationPreviousVersionMetric = explodeSyntheticIds(calibrationPreviousModelInferenceData)
                                            .groupBy('SyntheticId)
                                            .agg(avg('pred).alias("previous_avg_pred"))

    val calibrationMetric = if (dateTime.isAfter(Config.anchorStartDate.atStartOfDay())) {
                              currentTmpEmbeddingTable
                                      .join(calibrationCurrentVersionMetric, Seq("SyntheticId"), "left")
                                      .join(calibrationPreviousVersionMetric, Seq("SyntheticId"), "left")
                                      .join(previousEmbeddingTable, Seq("SyntheticId"), "left")
                                      .withColumn("BlendedAvgScore", (lit(Config.recursiveWeight) * 'previous_avg_pred + (lit(1.0)-lit(Config.recursiveWeight)) * 'BlendedAvgScore))
                                      .withColumn("CalibrationFactor", 'current_avg_pred / 'BlendedAvgScore)
                                      .select('SyntheticId, 'CalibrationFactor, 'BlendedAvgScore)
                            } else {
                              currentTmpEmbeddingTable
                                      .join(calibrationCurrentVersionMetric, Seq("SyntheticId"), "left")
                                      .join(calibrationPreviousVersionMetric, Seq("SyntheticId"), "left")
                                      .withColumn("BlendedAvgScore", lit(1.0) * 'previous_avg_pred )
                                      .withColumn("CalibrationFactor", 'current_avg_pred / 'BlendedAvgScore)

                                      .select('SyntheticId, 'CalibrationFactor, 'BlendedAvgScore)
                            }
    
    val minMaxMetric = explodeSyntheticIds(populationInferenceData)
                          .groupBy('SyntheticId).agg(
                            max('pred).alias("max_pred"),
                            min('pred).alias("min_pred")                                  
                          )
    
    val populationMetric = explodeSyntheticIds(populationInferenceData)
                              .join(minMaxMetric,Seq("SyntheticId"),"left")
                              .withColumn("pred", when('min_pred.isNotNull, least(greatest('pred-'min_pred,lit(0))/('max_pred-'min_pred),lit(1.0) )).otherwise('pred))
                              .withColumn("r", lit(Config.baselineHitRate))
                              .withColumn("weights",('r + (lit(1)-'r)*(lit(1)- lit(1)/(lit(1)+exp(-(lit(-Config.smoothFactor)*('pred-lit(Config.locationFactor))))))))
                              .withColumn("pred", 'pred*'weights)
                              .groupBy("SyntheticId")
                              .agg(
                                avg('pred).alias("PopulationRelevance")
                                , avg(log('pred)).alias("LogPopulationRelevance")
                                , avg('min_pred).alias("MinScore")
                                , avg('max_pred).alias("MaxScore")
                              ).select('SyntheticId, 'PopulationRelevance, 'LogPopulationRelevance, 'MinScore, 'MaxScore)
    
    val finalEmbeddingTable = currentTmpEmbeddingTable
                                                  .join(calibrationMetric, Seq("SyntheticId"), "left")
                                                  .join(populationMetric, Seq("SyntheticId"), "left")
                                                  .withColumn("BaselineHitRate", lit(Config.baselineHitRate))
                                                  .withColumn("LocationFactor", lit(Config.locationFactor))
                                                  .withColumn("CalibrationFactor", col("CalibrationFactor").cast(FloatType))
                                                  .withColumn("MinScore", col("MinScore").cast(FloatType))
                                                  .withColumn("MaxScore", col("MaxScore").cast(FloatType))
                                                  .withColumn("PopulationRelevance", col("PopulationRelevance").cast(FloatType))
                                                  .withColumn("LogPopulationRelevance", col("LogPopulationRelevance").cast(FloatType))
                                                  .withColumn("BaselineHitRate", col("BaselineHitRate").cast(FloatType))
                                                  .withColumn("LocationFactor", col("LocationFactor").cast(FloatType))
    
    finalEmbeddingTable.repartition(1).write.option("compression", "gzip").mode(Config.writeMode).parquet(outputEmbeddingPath)

  }
}
