package com.thetradedesk.audience.jobs

import com.thetradedesk.audience.configs.AudienceModelInputGeneratorConfig
import com.thetradedesk.geronimo.shared.transform.ModelFeatureTransform
import com.thetradedesk.audience.datasets._
import com.thetradedesk.audience.transform.ContextualTransform.generateContextualFeatureTier1
import com.thetradedesk.audience.utils.Logger.Log
import com.thetradedesk.audience.{date, dateTime, _}
import com.thetradedesk.geronimo.shared.readModelFeatures
import com.thetradedesk.audience.jobs.HitRateReportingTableGeneratorJob.prometheus
import com.thetradedesk.geronimo.bidsimpression.schema.{BidsImpressions, BidsImpressionsSchema}
import com.thetradedesk.geronimo.shared.{GERONIMO_DATA_SOURCE, loadParquetData}
import com.thetradedesk.spark.TTDSparkContext.spark
import com.thetradedesk.spark.TTDSparkContext.spark.implicits._
import com.thetradedesk.spark.util.TTDConfig.{config, defaultCloudProvider}
import com.thetradedesk.spark.util.io.FSUtils
import com.thetradedesk.spark.util.prometheus.PrometheusClient
import org.apache.commons.lang3.exception.ExceptionUtils
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.apache.spark.sql.{DataFrame, Dataset, Row, SaveMode, SparkSession}
import com.thetradedesk.audience.jobs.modelinput.rsmv2.usersampling.SamplerFactory
import com.thetradedesk.audience.datasets.S3Roots.ML_PLATFORM_ROOT
import com.thetradedesk.audience.utils.DataFrameUtils._

import java.time.format.DateTimeFormatter
import java.time.temporal.ChronoUnit
import java.time.{LocalDate, LocalDateTime}
import scala.util.Random

object RSMV2TrainingDataStatusGeneratorJob {
  val prometheus = new PrometheusClient("DataQualityCheckJob", "DataQualityCheckJob")

  def main(args: Array[String]): Unit = {
    runETLPipeline()
    prometheus.pushMetrics()
  }

  def runETLPipeline(): Unit = {
    val ttdOwnDataSmallTrainMeta = new DataQualityGenerator("TTDOwnDataSmall", "Train").generate(date)
    val ttdOwnDataNewTrainMeta = new DataQualityGenerator("TTDOwnDataNew", "Train").generate(date)
    val seedSmallTrainMeta = new DataQualityGenerator("SeedSmall", "Train").generate(date)
    val seedNewTrainMeta = new DataQualityGenerator("SeedNew", "Train").generate(date)

    val metaData = ttdOwnDataSmallTrainMeta.union(ttdOwnDataNewTrainMeta).union(seedSmallTrainMeta).union(seedNewTrainMeta).drop("weightedPosRatio")

    dateTime = date.atStartOfDay()

    TrainingMetaDataSet()
          .writePartition(
            metaData.as[TrainingMetaDataRecord],
            dateTime,
            saveMode = SaveMode.Overwrite
          )
  }
}

class DataQualityGenerator(val dataType: String, val dataSplitType: String) {

  def generate(date: LocalDate): DataFrame = {

    val formatter = DateTimeFormatter.ofPattern("yyyyMMdd")

    val dateString = date.format(formatter) + "000000"

    val start = System.currentTimeMillis()

    val trainData = spark.read.format("tfrecord").load(s"${ML_PLATFORM_ROOT}/${config.getString("TrainingDataReadEnv", ttdEnv)}/audience/RSMV2/Seed_None/v=1/${dateString}/${dataType}=${dataSplitType}/"
    ).select('SyntheticIds, 'Targets, 'ZipSiteLevel_Seed)

    val explodCols = Seq("SyntheticIds", "Targets", "ZipSiteLevel_Seed")
    val data = explodeColumns(trainData, explodCols)
                .withColumnRenamed("SyntheticIds", "SyntheticId")
                .withColumnRenamed("Targets", "Target")
                .withColumnRenamed("ZipSiteLevel_Seed", "ZipSiteLevel_Seed_TDID")
    
    val policyTable = AudienceModelPolicyReadableDataset(AudienceModelInputGeneratorConfig.model)
          .readSinglePartition(dateTime)(spark)
          .select("SyntheticId", "ActiveSize")

    val baseAgg = data
      .groupBy('SyntheticId)
      .agg(
        avg('Target).alias("posRatio"),
        count("*").alias("cnt")
      )

    val posRatiosByLevel = data
      .groupBy("SyntheticId", "ZipSiteLevel_Seed_TDID")
      .agg(avg('Target).alias("posRatio"))
      .groupBy("SyntheticId")
      .pivot("ZipSiteLevel_Seed_TDID", Seq("1", "2", "3"))
      .agg(first("posRatio"))
      .withColumnRenamed("1", "Density1PosRatio")
      .withColumnRenamed("2", "Density2PosRatio")
      .withColumnRenamed("3", "Density3PosRatio")

    val dataSyntheticId = baseAgg
      .join(policyTable, Seq("SyntheticId"), "inner")
      .withColumn("weightedPosRatio", 'ActiveSize * 'posRatio)
      .join(posRatiosByLevel, Seq("SyntheticId"), "left")
      .withColumn("DataType", lit(dataType + "=" + dataSplitType))

    // val dataDensity = data
    //   .groupBy("ZipSiteLevel_Seed_TDID")
    //   .agg(avg("Target").alias("posRatio"))
    //   .withColumn("SyntheticId", lit(-1)) // add dummy key
    //   .groupBy("SyntheticId")
    //   .pivot("ZipSiteLevel_Seed_TDID", Seq("1", "2", "3"))
    //   .agg(first("posRatio"))
    //   .withColumnRenamed("1", "Density1PosRatio")
    //   .withColumnRenamed("2", "Density2PosRatio")
    //   .withColumnRenamed("3", "Density3PosRatio")

    // val quantiles = dataSyntheticId.stat.approxQuantile("cnt", Array(0.25, 0.5, 0.75), 0.01)
    // val percentile25 = quantiles(0)
    // val percentile50 = quantiles(1)
    // val percentile75 = quantiles(2)

    // val weightedPosRatio = dataSyntheticId
    //   .agg(
    //     sum('weightedPosRatio).alias("total_weighted_pos"),
    //     sum('Size).alias("total_size"),
    //     avg('posRatio).alias("avgPosRatio")
    //   )
    //   .withColumn("weighted_pos_ratio", $"total_weighted_pos" / $"total_size")
    //   .select("weighted_pos_ratio", "avgPosRatio")


    // val result = dataDensity.crossJoin(weightedPosRatio)
    //             .withColumn("percentile25", lit(percentile25))
    //             .withColumn("percentile50", lit(percentile50))
    //             .withColumn("percentile75", lit(percentile75))
    //             .withColumn("DataType", lit(dataType + "=" + dataSplitType))

    // (result, dataSyntheticId)

    dataSyntheticId

  }
}
