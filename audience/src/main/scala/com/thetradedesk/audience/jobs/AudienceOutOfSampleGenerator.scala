package com.thetradedesk.audience.jobs

import com.thetradedesk.audience.datasets._
import com.thetradedesk.audience.{date, dateTime, shouldConsiderTDID3, ttdEnv}
import com.thetradedesk.geronimo.bidsimpression.schema.{BidsImpressions, BidsImpressionsSchema}
import com.thetradedesk.spark.util.TTDConfig.{config, defaultCloudProvider}
import com.thetradedesk.spark.TTDSparkContext.spark
import com.thetradedesk.spark.TTDSparkContext.spark.implicits._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.{IntegerType, StringType, StructField, StructType}
import org.apache.spark.sql.{Dataset, SaveMode, SparkSession}
import com.thetradedesk.audience.transform.ModelFeatureTransform
import org.apache.spark.sql.{DataFrame, Row}
import com.thetradedesk.audience.utils.S3Utils
import com.thetradedesk.audience.jobs.AudienceModelInputGeneratorJob.clusterTargetingData
import com.thetradedesk.audience.jobs.RSMSeedInputGenerator
import com.thetradedesk.audience.sample.WeightSampling.{getLabels, zipAndGroupUDFGenerator}
import com.thetradedesk.audience.sample.RandomSampling.negativeSampleUDFGenerator

import java.time.LocalDate
import java.time.LocalDateTime
import java.time.format.DateTimeFormatter
import java.util.concurrent.ThreadLocalRandom

object RSM_OOS_Config {
    val model = Model.withName(config.getString("modelName", "RSM"))

    val negativeSampleRatio = config.getInt("negativeSampleRatio", default = 10)

    val supportedDataSources = config.getString("supportedDataSources", "Seed").split(',')
      .map(dataSource => DataSource.withName(dataSource).id)

    var seedSizeLowerScaleThreshold = config.getInt("seedSizeLowerScaleThreshold", default = 1)

    var seedSizeUpperScaleThreshold = config.getInt("seedSizeUpperScaleThreshold", default = 12)

    val labelMaxLength = config.getInt("labelMaxLength", default = 50)

    val bidImpressionRepartitionNumAfterFilter = config.getInt("bidImpressionRepartitionNumAfterFilter", 8192)

    val seedRawDataRecentVersion = config.getString("seedRawDataRecentVersion", "None")

    val seedRawDataS3Bucket = S3Utils.refinePath(config.getString("seedRawDataS3Bucket", "ttd-datprd-us-east-1"))

    val seedRawDataS3Path = S3Utils.refinePath(config.getString("seedRawDataS3Path", "prod/data/Seed/v=1/SeedId="))

    val seedCoalesceAfterFilter = config.getInt("seedCoalesceAfterFilter", 3)

    val saltSampleOutSeed = config.getString(s"saltSampleOutSeed", default = "OOS_out_of_seed")

    // this is good to setted as same as labelMaxLength
    val outSeedNegSamples = config.getInt("outSeedNegSamples", 50)

    val workTask = config.getString("workTask", "OOS")

    var subFolder = config.getString("subFolder", "split")

    // extra 10% down sampling for out seed tdid -> 0.1 will lead to less than 1/3 data belongs to out seed given labemmaxlength = 50
    var outSeedSampleRatio = config.getDouble("outSeedSampleRatio", 0.1)

    // n bid impressions we care about
    val lastTouchNumberInBR = config.getInt("lastTouchNumberInBR", 3)

    // the way to determine the n tdid selection->
    // 0: last n tdid; 1: even stepwise selection for n tdid; 2 and other: random select n tdid
    val tdidTouchSelection = config.getInt("tdidTouchSelection", default = 0)

    // extra x% down sample after union out seed and in seed
    var extraSampleRatio = config.getDouble("extraSampleRatio", 1.0)

    val extraSaltSample = config.getString(s"extraSaltSample", default = "extra_sampling_further_downsample")
  }

object OutOfSampleGenerateJob {
  def main(args: Array[String]): Unit = {
    runETLPipeline()
  }

  def runETLPipeline(): Unit = {
      val policyTable = clusterTargetingData(RSM_OOS_Config.model, RSM_OOS_Config.supportedDataSources, RSM_OOS_Config.seedSizeLowerScaleThreshold, RSM_OOS_Config.seedSizeUpperScaleThreshold)
      val date = dateTime.toLocalDate
      // 10% more downsampling on the original 10% downsampling to make the size of out of seed tdid smaller
      var samplingFunction = shouldConsiderTDID3((RSM_OOS_Config.outSeedSampleRatio*1000000).toInt, RSM_OOS_Config.saltSampleOutSeed)(_)
      val (sampledBidsImpressionsKeys, bidsImpressionsLong, uniqueTDIDs) = RSMSeedInputGenerator.getBidImpressions(date, RSM_OOS_Config.lastTouchNumberInBR,
      RSM_OOS_Config.tdidTouchSelection)
      policyTable.foreach(typePolicyTable => {
      val dataset = {
        RSM_OOS_Config.model match {
          case Model.RSM => typePolicyTable match {
            case ((DataSource.Seed, CrossDeviceVendor.None), subPolicyTable: Array[AudienceModelPolicyRecord]) => {
              val rawLabels = RSMSeedInputGenerator.generateLabels(date, subPolicyTable,
              seedRawDataS3Path=RSM_OOS_Config.seedRawDataS3Path,
              seedRawDataS3Bucket=RSM_OOS_Config.seedRawDataS3Bucket,
              seedRawDataRecentVersion=RSM_OOS_Config.seedRawDataRecentVersion,
              seedCoalesceAfterFilter=RSM_OOS_Config.seedCoalesceAfterFilter,
              bidImpressionRepartitionNumAfterFilter=RSM_OOS_Config.bidImpressionRepartitionNumAfterFilter)
              // in seed oos
              val filteredInSeedLabels = uniqueTDIDs.join(rawLabels, Seq("TDID"), "inner")
              val refinedInSeedLabels = OOSSampling.inSeedSampleLabels(filteredInSeedLabels, subPolicyTable)
              val inSeedOOS = bidsImpressionsLong.drop("CampaignId", "LogEntryTime")
                .join(
                  refinedInSeedLabels
                    .join(
                      sampledBidsImpressionsKeys, Seq("TDID"), "inner"),
                  Seq("TDID", "BidRequestId"), "inner")

              // out seed oos
              val filteredOutSeedLabels = uniqueTDIDs.join(rawLabels, Seq("TDID"), "left_anti")
              val refinedOutSeedLabels = OOSSampling.outSeedSampleLabels(filteredOutSeedLabels, subPolicyTable, samplingFunction)
              val outSeedOOS = bidsImpressionsLong.drop("CampaignId", "LogEntryTime")
                .filter(samplingFunction('TDID))
                .join(
                  refinedOutSeedLabels
                    .join(
                      sampledBidsImpressionsKeys, Seq("TDID"), "inner"),
                  Seq("TDID", "BidRequestId"), "inner")

              var OOS: DataFrame = inSeedOOS.union(outSeedOOS)
              if (RSM_OOS_Config.extraSampleRatio != 1.0) {
                var extraSampling = shouldConsiderTDID3((RSM_OOS_Config.extraSampleRatio*1000000).toInt, RSM_OOS_Config.extraSaltSample)(_)
                OOS = OOS.filter(extraSampling('TDID))
              }
              OOS
            }
            case _ => throw new Exception(s"unsupported policy settings: Model[${Model.RSM}], Setting[${typePolicyTable._1}]")
          }
          case _ => throw new Exception(s"unsupported Model[${RSM_OOS_Config.model}]")
        }
      }
      val resultTransformed = ModelFeatureTransform.modelFeatureTransform[AudienceModelInputRecord](dataset)
      AudienceModelInputDataset(RSM_OOS_Config.model.toString, s"${typePolicyTable._1._1}_${typePolicyTable._1._2}_${RSM_OOS_Config.workTask}").writePartition(
            resultTransformed.as[AudienceModelInputRecord],
            dateTime,
            subFolderKey = Some(RSM_OOS_Config.subFolder),
            subFolderValue = Some(RSM_OOS_Config.workTask),
            format = Some("tfrecord"),
            saveMode = SaveMode.Overwrite
          )
      })
  }
}

object OOSSampling {
  def inSeedSampleLabels(labels: DataFrame, policyTable: Array[AudienceModelPolicyRecord]): DataFrame = {
    val negativeSampleUDF = negativeSampleUDFGenerator(
        policyTable,
        )

    // downsample positive labels to keep # of positive labels among targets balanced
    val labelResult = labels
        .withColumn("PositiveSamples", 'PositiveSyntheticIds)
        .withColumn("NegativeSamples", negativeSampleUDF(lit(RSM_OOS_Config.negativeSampleRatio) * size(col("PositiveSamples"))))
        .withColumn("NegativeSamples", array_except(col("NegativeSamples"), 'PositiveSyntheticIds))
        .withColumn("PositiveTargets", getLabels(1f)(size(col("PositiveSamples"))))
        .withColumn("NegativeTargets", getLabels(0f)(size(col("NegativeSamples"))))
        .withColumn("SyntheticIds", concat($"PositiveSamples", $"NegativeSamples"))
        .withColumn("Targets", concat($"PositiveTargets", $"NegativeTargets"))
        .select('TDID, 'SyntheticIds, 'Targets)
        // partialy explode the result to keep the target array within the max length
        .withColumn("ZippedTargets", zipAndGroupUDFGenerator(RSM_OOS_Config.labelMaxLength)('SyntheticIds, 'Targets))
        .select(col("TDID"), explode(col("ZippedTargets")).as("ZippedTargets"))
        .select(col("TDID"), col("ZippedTargets").getField("_1").as("SyntheticIds"), col("ZippedTargets").getField("_2").as("Targets"))

    labelResult
}

  def outSeedSampleLabels(labels: DataFrame, policyTable: Array[AudienceModelPolicyRecord], samplingMethod: Symbol => org.apache.spark.sql.Column): 
  DataFrame = {
    val negativeSampleUDF = negativeSampleUDFGenerator(
      policyTable
      )
    // downsample positive labels to keep # of positive labels among targets balanced
    val labelResult = labels
      .filter(samplingMethod('TDID))
      .withColumn("SyntheticIds", negativeSampleUDF(lit(RSM_OOS_Config.outSeedNegSamples)))
      .withColumn("Targets", getLabels(0f)(size(col("SyntheticIds"))))
      .select('TDID, 'SyntheticIds, 'Targets)

    labelResult
  }
}
