package com.thetradedesk.audience.jobs

import com.thetradedesk.audience.configs.AudienceModelInputGeneratorConfig
import com.thetradedesk.audience.datasets.CrossDeviceVendor.CrossDeviceVendor
import com.thetradedesk.audience.datasets._
import com.thetradedesk.audience.jobs.modelinput.AudienceModelInputGeneratorJob.clusterTargetingData
import com.thetradedesk.audience.jobs.modelinput.MultipleIdTypesSupporter.mergeLabels
import com.thetradedesk.audience.jobs.modelinput.RSMSeedInputGenerator
import com.thetradedesk.audience.sample.RandomSampling.negativeSampleUDFGenerator
import com.thetradedesk.audience.sample.WeightSampling.{getLabels, zipAndGroupUDFGenerator}
import com.thetradedesk.geronimo.shared.transform.ModelFeatureTransform
import com.thetradedesk.audience.transform.ContextualTransform
import com.thetradedesk.audience.transform.IDTransform.{allIdWithType, joinOnIdTypes}
import com.thetradedesk.audience.{dateTime, featuresJsonDestPath, featuresJsonSourcePath, shouldConsiderTDID3, userDownSampleBasePopulation}
import com.thetradedesk.geronimo.shared.readModelFeatures
import com.thetradedesk.spark.TTDSparkContext.spark.implicits._
import com.thetradedesk.spark.util.TTDConfig.config
import org.apache.spark.sql.{DataFrame, SaveMode}
import org.apache.spark.sql.functions._

import scala.reflect.runtime.universe._

object AudienceModel_OOS_Config {
  val negativeSampleRatio = config.getInt("negativeSampleRatio", default = 10)

  val supportedDataSources = config.getString("supportedDataSources", "Seed,TTDOwnData").split(',')
    .map(dataSource => DataSource.withName(dataSource).id)

  val saltSampleOutSeed = config.getString(s"saltSampleOutSeed", default = "OOS_out_of_seed")

  // this is good to setted as same as labelMaxLength
  val outSeedNegSamples = config.getInt("outSeedNegSamples", 50)

  val workTask = config.getString("workTask", "OOS")

  var subFolder = config.getString("subFolder", "split")

  // extra 10% down sampling for out seed tdid -> 0.1 will lead to less than 1/3 data belongs to out seed given labemmaxlength = 50
  var outSeedSampleRatio = config.getDouble("outSeedSampleRatio", 0.1)

  // extra x% down sample after union out seed and in seed
  var extraSampleRatio = config.getDouble("extraSampleRatio", 1.0)

  val extraSaltSample = config.getString(s"extraSaltSample", default = "extra_sampling_further_downsample")
}

object OutOfSampleGenerateJob {
  def main(args: Array[String]): Unit = {
    runETLPipeline()
  }

  def runETLPipeline(): Unit = {
    val date = dateTime.toLocalDate
    // val schedule = if (AudienceModelInputGeneratorConfig.IncrementalTrainingEnabled) DateUtils.getSchedule(date, AudienceModelInputGeneratorConfig.fullTrainDay, AudienceModelInputGeneratorConfig.trainingCadence) else Schedule.Full
    val policyTable = clusterTargetingData(AudienceModelInputGeneratorConfig.model, AudienceModel_OOS_Config.supportedDataSources, AudienceModelInputGeneratorConfig.supportedGraphs, AudienceModelInputGeneratorConfig.seedSizeUpperScaleThreshold, Schedule.Full)

    // 10% more downsampling on the original 10% downsampling to make the size of out of seed tdid smaller
    var samplingFunction = shouldConsiderTDID3((AudienceModel_OOS_Config.outSeedSampleRatio * userDownSampleBasePopulation).toInt, AudienceModel_OOS_Config.saltSampleOutSeed)(_)
    val (sampledBidsImpressionsKeys, bidsImpressionsLong) = RSMSeedInputGenerator(CrossDeviceVendor.None, 1.0).getBidImpressions(date, AudienceModelInputGeneratorConfig.lastTouchNumberInBR,
      AudienceModelInputGeneratorConfig.tdidTouchSelection)

    policyTable.foreach(typePolicyTable => {
      val dataset = {
        AudienceModelInputGeneratorConfig.model match {
          case Model.RSM => typePolicyTable match {
            case ((_, crossDeviceVendor: CrossDeviceVendor, IncrementalTrainingTag.Full), subPolicyTable: Array[AudienceModelPolicyRecord]) => {
              val rawLabels = new RSMSeedInputGenerator(crossDeviceVendor, 1.0).generateLabels(date, subPolicyTable)
              // in seed oos
              val refinedInSeedLabels = OOSSampling.inSeedSampleLabels(rawLabels, subPolicyTable)
              val mergedInSeedLabels = mergeLabels(joinOnIdTypes(sampledBidsImpressionsKeys, refinedInSeedLabels)).cache()

              val inSeedOOS = bidsImpressionsLong.drop("LogEntryTime", "TDID")
                .repartition(16384, 'BidRequestId)
                .join(
                  mergedInSeedLabels,
                  Seq("BidRequestId"), "inner")

              // multiple anti_left need to merge them together
              // change to mergedLabels and use BidRequestId to join to speed up
              val uniqueTDIDs = sampledBidsImpressionsKeys
                .select('BidRequestId, allIdWithType.alias("x"))
                .select('BidRequestId, col("x._1").as("TDID"), col("x._2").as("idType"))
                .filter(samplingFunction('TDID))
                .distinct()

              val filteredOutSeedLabels = uniqueTDIDs.join(mergedInSeedLabels.filter(samplingFunction('TDID)), Seq("BidRequestId"), "left_anti")
              val refinedOutSeedLabels = OOSSampling.outSeedSampleLabels(filteredOutSeedLabels, subPolicyTable)
              val mergedOutSeedLabels = mergeLabels(
                joinOnIdTypes(
                  sampledBidsImpressionsKeys.filter(samplingFunction('TDID)),
                  refinedOutSeedLabels))
                .cache()
              val outSeedOOS = bidsImpressionsLong
                .filter(samplingFunction('TDID))
                .drop("LogEntryTime", "TDID")
                .join(
                  mergedOutSeedLabels,
                  Seq("BidRequestId")
                  , "inner"
                )

              var OOS: DataFrame = inSeedOOS.union(outSeedOOS)
              if (AudienceModel_OOS_Config.extraSampleRatio != 1.0) {
                val extraSampling = shouldConsiderTDID3((AudienceModel_OOS_Config.extraSampleRatio * userDownSampleBasePopulation).toInt, AudienceModel_OOS_Config.extraSaltSample)(_)
                OOS = OOS.filter(extraSampling('TDID))
              }
              ContextualTransform.generateContextualFeatureTier1(OOS.withColumn("GroupID", 'TDID))
            }
            case _ => throw new Exception(s"unsupported policy settings: Model[${Model.RSM}], Setting[${typePolicyTable._1}]")
          }
          case _ => throw new Exception(s"unsupported Model[${AudienceModelInputGeneratorConfig.model}]")
        }
      }

      val resultTransformed = ModelFeatureTransform.modelFeatureTransform[AudienceModelInputRecord](dataset, readModelFeatures(featuresJsonSourcePath))
      AudienceModelInputDataset(AudienceModelInputGeneratorConfig.model.toString, s"${typePolicyTable._1._1}_${typePolicyTable._1._2}").writePartition(
        resultTransformed.as[AudienceModelInputRecord],
        dateTime,
        subFolderKey = Some(AudienceModel_OOS_Config.subFolder),
        subFolderValue = Some(AudienceModel_OOS_Config.workTask),
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
      .withColumn("NegativeSamples", negativeSampleUDF(lit(AudienceModel_OOS_Config.negativeSampleRatio) * size(col("PositiveSamples"))))
      .withColumn("NegativeSamples", array_except(col("NegativeSamples"), 'PositiveSyntheticIds))
      .withColumn("PositiveTargets", getLabels(TypeTag.Boolean)(true)(size(col("PositiveSamples"))))
      .withColumn("NegativeTargets", getLabels(TypeTag.Boolean)(false)(size(col("NegativeSamples"))))
      .withColumn("SyntheticIds", concat($"PositiveSamples", $"NegativeSamples"))
      .withColumn("Targets", concat($"PositiveTargets", $"NegativeTargets"))
      .select('TDID, 'idType, 'SyntheticIds, 'Targets)
      // partialy explode the result to keep the target array within the max length
      .withColumn("ZippedTargets", zipAndGroupUDFGenerator(AudienceModelInputGeneratorConfig.labelMaxLength)('SyntheticIds, 'Targets))
      .select(col("TDID"), col("idType"), explode(col("ZippedTargets")).as("ZippedTargets"))
      .select(col("TDID"), col("idType"), col("ZippedTargets").getField("_1").as("SyntheticIds"), col("ZippedTargets").getField("_2").as("Targets"))

    labelResult
  }

  def outSeedSampleLabels(labels: DataFrame, policyTable: Array[AudienceModelPolicyRecord]):
  DataFrame = {
    val negativeSampleUDF = negativeSampleUDFGenerator(
      policyTable
    )
    // downsample positive labels to keep # of positive labels among targets balanced
    val labelResult = labels
      .withColumn("SyntheticIds", negativeSampleUDF(lit(AudienceModel_OOS_Config.outSeedNegSamples)))
      .withColumn("Targets", getLabels(TypeTag.Boolean)(false)(size(col("SyntheticIds"))))
      .select('TDID, 'idType, 'SyntheticIds, 'Targets)

    labelResult
  }
}
