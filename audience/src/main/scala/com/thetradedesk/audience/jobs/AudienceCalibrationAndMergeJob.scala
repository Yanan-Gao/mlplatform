package com.thetradedesk.audience.jobs

import com.thetradedesk.audience.datasets.{AudienceModelPolicyReadableDataset, CrossDeviceVendor, Model}
import com.thetradedesk.audience.jobs.TdidEmbeddingDotProductGeneratorOOS.EmbeddingSize
import com.thetradedesk.audience.utils.S3Utils
import com.thetradedesk.audience.{audienceVersionDateFormat, date}
import com.thetradedesk.confetti.AutoConfigResolvingETLJobBase
import com.thetradedesk.spark.TTDSparkContext.spark
import com.thetradedesk.spark.TTDSparkContext.spark.implicits._
import com.thetradedesk.spark.util.TTDConfig.config
import com.thetradedesk.spark.util.prometheus.PrometheusClient
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.FloatType

import java.time.format.DateTimeFormatter
import java.time.{LocalDate, LocalDateTime}

case class AudienceCalibrationAndMergeJobConfig(
  model: String,
  mlplatformS3Bucket: String,
  tmpNonSenEmbeddingDataS3Path: String,
  tmpSenEmbeddingDataS3Path: String,
  embeddingDataS3Path: String,
  inferenceDataS3Path: String,
  embeddingRecentVersion: Option[String],
  anchorStartDate: LocalDate,
  smoothFactor: Double,
  locationFactor: Double,
  baselineHitRate: Double,
  recursiveWeight: Double,
  writeMode: String,
  runDate: LocalDate
)

object AudienceCalibrationAndMergeJob
  extends AutoConfigResolvingETLJobBase[AudienceCalibrationAndMergeJobConfig](
    groupName = "audience",
    jobName = "AudienceCalibrationAndMergeJob") {
  override val prometheus: Option[PrometheusClient] =
    Some(new PrometheusClient("AudienceModelJob", "AudienceCalibrationAndMergeJob"))

  override def runETLPipeline(): Map[String, String] = {
    val conf = getConfig
    val date = conf.runDate
    val dateTime = conf.runDate.atStartOfDay()

    val jobConf = conf.copy(
      mlplatformS3Bucket = S3Utils.refinePath(conf.mlplatformS3Bucket),
      tmpNonSenEmbeddingDataS3Path = S3Utils.refinePath(conf.tmpNonSenEmbeddingDataS3Path),
      tmpSenEmbeddingDataS3Path = S3Utils.refinePath(conf.tmpSenEmbeddingDataS3Path),
      embeddingDataS3Path = S3Utils.refinePath(conf.embeddingDataS3Path),
      inferenceDataS3Path = S3Utils.refinePath(conf.inferenceDataS3Path)
    )

    val formatter = DateTimeFormatter.ofPattern("yyyyMMdd")

    val embeddingTableDateFormatter = DateTimeFormatter.ofPattern(audienceVersionDateFormat)
    val availableEmbeddingVersions = S3Utils
    .queryCurrentDataVersions(jobConf.mlplatformS3Bucket, jobConf.embeddingDataS3Path)
    .map(LocalDateTime.parse(_, embeddingTableDateFormatter))
    .toSeq
    .sortWith(_.isAfter(_))

    val recentVersionOption = jobConf.embeddingRecentVersion
      .map(LocalDateTime.parse(_, embeddingTableDateFormatter))
      .orElse(availableEmbeddingVersions.find(_.isBefore(dateTime)))

    val recentVersionStr = recentVersionOption.get.asInstanceOf[java.time.LocalDateTime].toLocalDate.format(formatter)

    val previousPolicyTable = AudienceModelPolicyReadableDataset(Model.RSM)
      .readSinglePartition(recentVersionOption.get.asInstanceOf[java.time.LocalDateTime])(spark)
      .filter((col("CrossDeviceVendorId") === CrossDeviceVendor.None.id))
      .select("SyntheticId", "IsSensitive")
    val currentPolicyTable = AudienceModelPolicyReadableDataset(Model.RSM)
      .readSinglePartition(dateTime)(spark)
      .filter((col("CrossDeviceVendorId") === CrossDeviceVendor.None.id))
      .select("SyntheticId", "IsSensitive")
      
    val previousEmbeddingPath = s"s3://${jobConf.mlplatformS3Bucket}/${jobConf.embeddingDataS3Path}/${recentVersionStr}000000"
    val currentNonSenTmpEmbeddingPath = s"s3://${jobConf.mlplatformS3Bucket}/${jobConf.tmpNonSenEmbeddingDataS3Path}/${date.format(formatter)}000000"
    val currentSenTmpEmbeddingPath = s"s3://${jobConf.mlplatformS3Bucket}/${jobConf.tmpSenEmbeddingDataS3Path}/${date.format(formatter)}000000"
    
    val outputEmbeddingPath = config.getString("overrideEmbeddingPath", s"s3://${jobConf.mlplatformS3Bucket}/${jobConf.embeddingDataS3Path}/${date.format(formatter)}000000")

    val alignEmbeddingColumn = when(size('Embedding) === lit(EmbeddingSize * 4), slice('Embedding, EmbeddingSize + 1, EmbeddingSize * 3)).otherwise('Embedding)
    val embeddingCols = Seq("SyntheticId", "Embedding", "Thresholds", "Threshold", "AvgScore")
    val currentTmpNonSenEmbeddingTable = spark.read.parquet(currentNonSenTmpEmbeddingPath).withColumn("Embedding", alignEmbeddingColumn).select(embeddingCols.map(col): _*)
    val currentTmpSenEmbeddingTable = spark.read.parquet(currentSenTmpEmbeddingPath).withColumn("Embedding", alignEmbeddingColumn).select(embeddingCols.map(col): _*)
    
    val previousEmbeddingTable = spark.read.parquet(previousEmbeddingPath).withColumn("Embedding", alignEmbeddingColumn)
    

    val previousCalibrationPath = s"s3://${jobConf.mlplatformS3Bucket}/${jobConf.inferenceDataS3Path}/calibration_data/v=1/model_version=${recentVersionStr}000000/${date.format(formatter)}000000/"
    val currentCalibrationPath = s"s3://${jobConf.mlplatformS3Bucket}/${jobConf.inferenceDataS3Path}/calibration_data/v=1/model_version=${date.format(formatter)}000000/${date.format(formatter)}000000/"
    val populationPath = config.getString("overridePopulationPath", s"s3://${jobConf.mlplatformS3Bucket}/${jobConf.inferenceDataS3Path}/population_data/v=1/model_version=${date.format(formatter)}000000/${date.format(formatter)}000000/")

    val populationInferenceData = spark.read.parquet(populationPath)
    val calibrationCurrentModelInferenceData = spark.read.parquet(currentCalibrationPath)
    val calibrationPreviousModelInferenceData = spark.read.parquet(previousCalibrationPath)

    def explodeSyntheticIds(df: DataFrame): DataFrame = {
        df.withColumn("zipped", arrays_zip($"SyntheticIds", $"Targets", $"pred", $"sen_pred"))
          .withColumn("exploded", explode($"zipped"))
          .withColumn("SyntheticId", $"exploded.SyntheticIds")
          .withColumn("Target", $"exploded.Targets")
          .withColumn("pred", $"exploded.pred")
          .withColumn("sen_pred", $"exploded.sen_pred")
          .drop("zipped", "exploded")
      }

    val currentTmpMixedEmbeddingTable = currentTmpNonSenEmbeddingTable
                                          .join(currentPolicyTable, Seq("SyntheticId"), "left")
                                          .join(currentTmpSenEmbeddingTable
                                                .withColumnRenamed("Embedding", "SenEmbedding")
                                                .withColumnRenamed("Thresholds", "SenThresholds")
                                                .withColumnRenamed("Threshold", "SenThreshold")
                                                .withColumnRenamed("AvgScore", "SenAvgScore")     
                                                , Seq("SyntheticId"), "left"                                  
                                          )
                                          .withColumn("Embedding", when('IsSensitive, 'SenEmbedding).otherwise('Embedding))
                                          .withColumn("Thresholds", when('IsSensitive, 'SenThresholds).otherwise('Thresholds))
                                          .withColumn("Threshold", when('IsSensitive, 'SenThreshold).otherwise('Threshold))
                                          .withColumn("AvgScore", when('IsSensitive, 'SenAvgScore).otherwise('AvgScore))

    val calibrationCurrentVersionMetric = explodeSyntheticIds(calibrationCurrentModelInferenceData)
                                            .join(currentPolicyTable, Seq("SyntheticId"),"left")
                                            .withColumn("pred", when('IsSensitive, 'sen_pred).otherwise('pred))
                                            .groupBy("SyntheticId")
                                            .agg(avg("pred").alias("current_avg_pred"))

    val calibrationPreviousVersionMetric = explodeSyntheticIds(calibrationPreviousModelInferenceData)
                                            .join(previousPolicyTable, Seq("SyntheticId"),"left")
                                            .withColumn("pred", when('IsSensitive, 'sen_pred).otherwise('pred))
                                            .groupBy('SyntheticId)
                                            .agg(avg('pred).alias("previous_avg_pred"))

    val calibrationMetric = if (dateTime.isAfter(jobConf.anchorStartDate.atStartOfDay())) {
                              currentTmpMixedEmbeddingTable
                                      .join(calibrationCurrentVersionMetric, Seq("SyntheticId"), "left")
                                      .join(calibrationPreviousVersionMetric, Seq("SyntheticId"), "left")
                                      .join(previousEmbeddingTable, Seq("SyntheticId"), "left")
                                      .withColumn("BlendedAvgScore", (lit(jobConf.recursiveWeight) * 'previous_avg_pred + (lit(1.0)-lit(jobConf.recursiveWeight)) * 'BlendedAvgScore))
                                      .withColumn("CalibrationFactor", 'current_avg_pred / 'BlendedAvgScore)
                                      .select('SyntheticId, 'CalibrationFactor, 'BlendedAvgScore)
                            } else {
                              currentTmpMixedEmbeddingTable
                                      .join(calibrationCurrentVersionMetric, Seq("SyntheticId"), "left")
                                      .join(calibrationPreviousVersionMetric, Seq("SyntheticId"), "left")
                                      .withColumn("BlendedAvgScore", lit(1.0) * 'previous_avg_pred )
                                      .withColumn("CalibrationFactor", 'current_avg_pred / 'BlendedAvgScore)

                                      .select('SyntheticId, 'CalibrationFactor, 'BlendedAvgScore)
                            }
    
    val minMaxMetric = explodeSyntheticIds(populationInferenceData)
                          .join(currentPolicyTable, Seq("SyntheticId"),"left")
                          .withColumn("pred", when('IsSensitive, 'sen_pred).otherwise('pred))
                          .groupBy('SyntheticId).agg(
                            max('pred).alias("max_pred"),
                            min('pred).alias("min_pred")                                  
                          )
    
    val populationMetric = explodeSyntheticIds(populationInferenceData)
                              .join(currentPolicyTable, Seq("SyntheticId"),"left")
                              .withColumn("pred", when('IsSensitive, 'sen_pred).otherwise('pred))
                              .join(minMaxMetric,Seq("SyntheticId"),"left")
                              .withColumn("pred", when('min_pred.isNotNull, least(greatest('pred-'min_pred,lit(0))/('max_pred-'min_pred),lit(1.0) )).otherwise('pred))
                              .withColumn("r", lit(jobConf.baselineHitRate))
                              .withColumn("weights",'r + (lit(1)-'r)*(lit(1)- lit(1)/(lit(1)+exp(-(lit(-jobConf.smoothFactor)*('pred-lit(jobConf.locationFactor)))))))
                              .withColumn("pred", 'pred*'weights)
                              .groupBy("SyntheticId")
                              .agg(
                                avg('pred).alias("PopulationRelevance")
                                , avg(log('pred)).alias("LogPopulationRelevance")
                                , avg('min_pred).alias("MinScore")
                                , avg('max_pred).alias("MaxScore")
                              ).select('SyntheticId, 'PopulationRelevance, 'LogPopulationRelevance, 'MinScore, 'MaxScore)
    
    val finalEmbeddingTable = currentTmpMixedEmbeddingTable
                                                  .join(calibrationMetric, Seq("SyntheticId"), "left")
                                                  .join(populationMetric, Seq("SyntheticId"), "left")
                                                  .withColumn("BaselineHitRate", lit(jobConf.baselineHitRate))
                                                  .withColumn("LocationFactor", lit(jobConf.locationFactor))
                                                  .withColumn("CalibrationFactor", col("CalibrationFactor").cast(FloatType))
                                                  .withColumn("MinScore", col("MinScore").cast(FloatType))
                                                  .withColumn("MaxScore", col("MaxScore").cast(FloatType))
                                                  .withColumn("PopulationRelevance", col("PopulationRelevance").cast(FloatType))
                                                  .withColumn("LogPopulationRelevance", col("LogPopulationRelevance").cast(FloatType))
                                                  .withColumn("BaselineHitRate", col("BaselineHitRate").cast(FloatType))
                                                  .withColumn("LocationFactor", col("LocationFactor").cast(FloatType))
    
    finalEmbeddingTable.distinct().repartition(1).write.option("compression", "gzip").mode(jobConf.writeMode).parquet(outputEmbeddingPath)
    Map("status" -> "success")

  }
}
