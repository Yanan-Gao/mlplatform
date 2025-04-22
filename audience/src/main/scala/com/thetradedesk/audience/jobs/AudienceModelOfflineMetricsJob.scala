package com.thetradedesk.audience.jobs

import com.thetradedesk.audience.datasets.{AdGroupDataSet, ModelOfflineMetricsDataset, ModelOfflineMetricsRecord, AudienceModelPolicyReadableDataset, AudienceModelThresholdRecord, AudienceModelThresholdWritableDataset, CampaignSeedDataset, Model}
import com.thetradedesk.audience.jobs.AudienceCalibrationAndMergeJob.prometheus
import com.thetradedesk.audience.{date, dateTime, ttdReadEnv, ttdWriteEnv, audienceVersionDateFormat}
import com.thetradedesk.audience.utils.{S3Utils, precisionRecallAggregator, AUCPerGroupAggregator}
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
import com.thetradedesk.audience.utils.DataFrameUtils._

object AudienceModelOfflineMetricsGeneratorJob {
  val prometheus = new PrometheusClient("AudienceModelJob", "AudienceCalibrationAndMergeJob")

  object Config {
    val mlplatformS3Bucket = S3Utils.refinePath(config.getString("mlplatformS3Bucket", "thetradedesk-mlplatform-us-east-1"))
    val metricOutputS3Path = S3Utils.refinePath(config.getString("metricOutputS3Path", s"data/${ttdReadEnv}/audience/RSMV2/measurement/finalModelMetaData/v=1"))
    val inferenceDataS3Path = S3Utils.refinePath(config.getString("inferenceDataS3Path", s"data/${ttdReadEnv}/audience/RSMV2/prediction"))

    val prThreshold = config.getDouble("prThreshold", default = 0.5)
    val numAUCBuckets = config.getInt("numAUCBuckets", default = 100000)
    val writeMode = config.getString("writeMode", "overwrite")
  }

  def main(args: Array[String]): Unit = {
    runETLPipeline()
    prometheus.pushMetrics()
  }

  def runETLPipeline(): Unit = {

    val formatter = DateTimeFormatter.ofPattern("yyyyMMdd")
    val dateTime = date.atStartOfDay()
    val outputPath = s"s3://${Config.mlplatformS3Bucket}/${Config.metricOutputS3Path}/${date.format(formatter)}000000"

    val currentPolicyTable = AudienceModelPolicyReadableDataset(Model.RSM)
      .readSinglePartition(dateTime)(spark)
      .filter((col("CrossDeviceVendorId") === 0))
      .select("SyntheticId", "IsSensitive").distinct()

    val populationInferencePath = s"s3://${Config.mlplatformS3Bucket}/${Config.inferenceDataS3Path}/population_data/v=1/model_version=${date.format(formatter)}000000/${date.format(formatter)}000000/"
    val OOSInferencePath = s"s3://${Config.mlplatformS3Bucket}/${Config.inferenceDataS3Path}/oos_data/v=1/model_version=${date.format(formatter)}000000/${date.format(formatter)}000000/"

    val populationInferenceData = spark.read.parquet(populationInferencePath)
    val oosInferenceData = spark.read.parquet(OOSInferencePath)

    val prAgg = new precisionRecallAggregator(Config.prThreshold)
    val pr_udf = udaf(prAgg) 

    val aucAgg = new AUCPerGroupAggregator(Config.numAUCBuckets)
    val auc_udf = udaf(aucAgg)

    def aggregateMetrics(df: DataFrame, metricPrefix: String, groupColumns: Seq[String] = Seq.empty[String]): DataFrame = {
      
      val aggExprs: Seq[org.apache.spark.sql.Column] = Seq(
        count(col("pred")).alias(s"${metricPrefix}ImpCount"),
        auc_udf(col("pred"), col("Targets")).alias(s"${metricPrefix}AUC"),
        pr_udf(col("pred"), col("Targets")).alias("precision_recall"),
        stddev(col("pred")).alias(s"${metricPrefix}PredStddev"),
        (sum(col("Targets")) / count(col("pred"))).alias(s"${metricPrefix}PosRatio"),
        avg(when(col("Targets") === 1, col("pred")).otherwise(null)).alias(s"${metricPrefix}InSeedAvgPred"),
        avg(when(col("Targets") === 0, col("pred")).otherwise(null)).alias(s"${metricPrefix}OutSeedAvgPred"),
        avg(when(col("ZipSiteLevel_Seed") =!= 1, lit(null))
              .otherwise(when(col("Targets") === 1, lit(1)).otherwise(lit(0))))
              .alias(s"${metricPrefix}Density1PosRatio"),
        avg(when(col("ZipSiteLevel_Seed") =!= 2, lit(null))
              .otherwise(when(col("Targets") === 1, lit(1)).otherwise(lit(0))))
              .alias(s"${metricPrefix}Density2PosRatio"),
        avg(when(col("ZipSiteLevel_Seed") =!= 3, lit(null))
              .otherwise(when(col("Targets") === 1, lit(1)).otherwise(lit(0))))
              .alias(s"${metricPrefix}Density3PosRatio")
      )
      
      val aggDf = if (groupColumns.isEmpty) {
        df.agg(aggExprs.head, aggExprs.tail: _*)
      } else {
        df.groupBy(groupColumns.head, groupColumns.tail: _*).agg(aggExprs.head, aggExprs.tail: _*)
      }
      
      aggDf.withColumn(s"${metricPrefix}Precision", col("precision_recall._1"))
        .withColumn(s"${metricPrefix}Recall", col("precision_recall._2"))
        .drop("precision_recall")
    }

    val explodCols = Seq("SyntheticIds", "Targets", "pred", "sen_pred", "ZipSiteLevel_Seed")
    val populationInferenceDataExploded = explodeColumns(populationInferenceData, explodCols).withColumnRenamed("SyntheticIds", "SyntheticId")
                                            .join(currentPolicyTable, Seq("SyntheticId"),"left")
                                            .withColumn("pred", when('IsSensitive, 'sen_pred).otherwise('pred))
                                            .cache
    val oosInferenceDataExploded = explodeColumns(oosInferenceData, explodCols).withColumnRenamed("SyntheticIds", "SyntheticId")
                                            .join(currentPolicyTable, Seq("SyntheticId"),"left")
                                            .withColumn("pred", when('IsSensitive, 'sen_pred).otherwise('pred))
                                            .cache
    
    val popMetricPrefix = "Population"
    val populationMetric = aggregateMetrics(populationInferenceDataExploded, popMetricPrefix, Seq("SyntheticId"))

    val oosMetricPrefix = "OOS"
    val oosMetric = aggregateMetrics(oosInferenceDataExploded, oosMetricPrefix, Seq("SyntheticId"))                              
                                             
    val populationOverallMetric = aggregateMetrics(populationInferenceDataExploded, popMetricPrefix)
                                              .withColumn("SyntheticId", lit(-1))
                                          
    val oosOverallMetric = aggregateMetrics(oosInferenceDataExploded, oosMetricPrefix) 
                                              .withColumn("SyntheticId", lit(-1))

    
    val finalMetric = populationMetric.unionByName(populationOverallMetric)
                      .join(oosMetric.unionByName(oosOverallMetric), Seq("SyntheticId"), "outer")
    
    finalMetric.repartition(1).write.option("compression", "gzip").mode(Config.writeMode).parquet(outputPath)
            
  }
}
