package com.thetradedesk.audience.jobs.modelinput

import com.thetradedesk.audience._
import com.thetradedesk.audience.configs.AudienceModelInputGeneratorConfig
import com.thetradedesk.audience.datasets.CrossDeviceVendor.CrossDeviceVendor
import com.thetradedesk.audience.datasets._
import com.thetradedesk.audience.utils.DateUtils
import com.thetradedesk.geronimo.shared.readModelFeatures
import com.thetradedesk.geronimo.shared.transform.ModelFeatureTransform
import com.thetradedesk.spark.TTDSparkContext.spark
import com.thetradedesk.spark.TTDSparkContext.spark.implicits._
import com.thetradedesk.spark.util.io.FSUtils
import com.thetradedesk.spark.util.prometheus.PrometheusClient
import org.apache.spark.sql.SaveMode
import org.apache.spark.sql.functions._

import scala.util.Try

object AudienceModelInputGeneratorJob {
  val prometheus = new PrometheusClient("AudienceModelJob", "AudienceModelInputGeneratorJob")
  val jobRunningTime = prometheus.createGauge(s"audience_etl_job_running_time", "AudienceModelInputGeneratorJob running time", "model", "date")
  val jobProcessSize = prometheus.createGauge(s"audience_etl_job_process_size", "AudienceModelInputGeneratorJob process size", "model", "date", "data_source", "cross_device_vendor")
  val posRatioGauge = prometheus.createGauge(s"pos_ratio", "pos ratio value", "model")


  object SubFolder extends Enumeration {
    type SubFolder = Value
    val Val, Holdout, Train = Value
  }

  def main(args: Array[String]): Unit = {
    val start = System.currentTimeMillis()
    runETLPipeline()
    jobRunningTime.labels(AudienceModelInputGeneratorConfig.model.toString.toLowerCase, dateTime.toLocalDate.toString).set(System.currentTimeMillis() - start)
    prometheus.pushMetrics()
  }

  def runETLPipeline(): Unit = {
    val date = dateTime.toLocalDate
    val schedule = if (AudienceModelInputGeneratorConfig.IncrementalTrainingEnabled) DateUtils.getSchedule(date, AudienceModelInputGeneratorConfig.trainingStartDate, AudienceModelInputGeneratorConfig.trainingCadence) else Schedule.Full
    val policyTable = clusterTargetingData(AudienceModelInputGeneratorConfig.model, AudienceModelInputGeneratorConfig.supportedDataSources,
      AudienceModelInputGeneratorConfig.seedSizeLowerScaleThreshold, AudienceModelInputGeneratorConfig.seedSizeUpperScaleThreshold, schedule)

    policyTable.foreach(typePolicyTable => {
      val dataset = {
        val result = AudienceModelInputGeneratorConfig.model match {
          case Model.AEM =>
            schedule match {
              case Schedule.Full =>
                typePolicyTable match {
                  case ((DataSource.SIB, CrossDeviceVendor.None, IncrementalTrainingTag.Full), subPolicyTable: Array[AudienceModelPolicyRecord]) =>
                    AEMSIBInputGenerator(1.0).generateDataset(date, subPolicyTable)
                  case ((DataSource.Conversion, crossDeviceVendor: CrossDeviceVendor, IncrementalTrainingTag.Full), subPolicyTable: Array[AudienceModelPolicyRecord]) =>
                    AEMConversionGraphInputGenerator(crossDeviceVendor, 1.0).generateDataset(date, subPolicyTable)

                  case _ => throw new Exception(s"unsupported policy settings: Model[${Model.AEM}], Setting[${typePolicyTable._1}]")
                }
              case Schedule.Incremental =>
                typePolicyTable match {
                  case ((DataSource.SIB, CrossDeviceVendor.None, IncrementalTrainingTag.New), subPolicyTable: Array[AudienceModelPolicyRecord]) =>
                    AEMSIBInputGenerator(1.0).generateDataset(date, subPolicyTable)
                  case ((DataSource.SIB, CrossDeviceVendor.None, IncrementalTrainingTag.Small), subPolicyTable: Array[AudienceModelPolicyRecord]) =>
                    AEMSIBInputGenerator(AudienceModelInputGeneratorConfig.IncrementalTrainingSampleRate).generateDataset(date, subPolicyTable)
                  case ((DataSource.Conversion, crossDeviceVendor: CrossDeviceVendor, IncrementalTrainingTag.New), subPolicyTable: Array[AudienceModelPolicyRecord]) =>
                    AEMConversionGraphInputGenerator(crossDeviceVendor, 1.0).generateDataset(date, subPolicyTable)
                  case ((DataSource.Conversion, crossDeviceVendor: CrossDeviceVendor, IncrementalTrainingTag.Small), subPolicyTable: Array[AudienceModelPolicyRecord]) =>
                    AEMConversionGraphInputGenerator(crossDeviceVendor, AudienceModelInputGeneratorConfig.IncrementalTrainingSampleRate).generateDataset(date, subPolicyTable)
                  case _ => throw new Exception(s"unsupported policy settings: Model[${Model.AEM}], Setting[${typePolicyTable._1}]")
                }
              case _ => throw new Exception(s"unsupported training schedule: Model[${Model.AEM}], Schedule[${schedule}]")
            }

          case Model.RSM =>
            schedule match {
              case Schedule.Full =>
                typePolicyTable match {
                  case ((DataSource.Seed, crossDeviceVendor: CrossDeviceVendor, IncrementalTrainingTag.Full), subPolicyTable: Array[AudienceModelPolicyRecord]) =>
                    RSMSeedInputGenerator(crossDeviceVendor,1.0).generateDataset(date, subPolicyTable)
                  case _ => throw new Exception(s"unsupported policy settings: Model[${Model.RSM}], Setting[${typePolicyTable._1}]")
                }
              case Schedule.Incremental =>
                typePolicyTable match {
                  case ((DataSource.Seed, crossDeviceVendor: CrossDeviceVendor, IncrementalTrainingTag.New), subPolicyTable: Array[AudienceModelPolicyRecord]) =>
                    RSMSeedInputGenerator(crossDeviceVendor, 1.0).generateDataset(date, subPolicyTable)
                  case ((DataSource.Seed, crossDeviceVendor: CrossDeviceVendor, IncrementalTrainingTag.Small), subPolicyTable: Array[AudienceModelPolicyRecord]) =>
                    RSMSeedInputGenerator(crossDeviceVendor, AudienceModelInputGeneratorConfig.IncrementalTrainingSampleRate).generateDataset(date, subPolicyTable)
                  case _ => throw new Exception(s"unsupported policy settings: Model[${Model.RSM}], Setting[${typePolicyTable._1}]")
                }

            }

          case _ => throw new Exception(s"unsupported Model[${AudienceModelInputGeneratorConfig.model}]")
        }
        jobProcessSize
          .labels(AudienceModelInputGeneratorConfig.model.toString.toLowerCase, dateTime.toLocalDate.toString, typePolicyTable._1._1.toString, typePolicyTable._1._2.toString)
          .set(typePolicyTable._2.length)

        result
      }

      val rawJson = readModelFeatures(featuresJsonSourcePath)()
      // write the rawJson to destination if present.
      if (featuresJsonDestPath != null && rawJson != null) {
        FSUtils.writeStringToFile(featuresJsonDestPath, rawJson)(spark)
      }

      val resultSet = ModelFeatureTransform.modelFeatureTransform[AudienceModelInputRecord](dataset, rawJson)
        .withColumn("SplitRemainder", xxhash64(concat('GroupId, lit(AudienceModelInputGeneratorConfig.saltToSplitDataset))) % AudienceModelInputGeneratorConfig.validateDatasetSplitModule)
        .withColumn("SubFolder",
          when('SplitRemainder === lit(SubFolder.Val.id), SubFolder.Val.id)
            .when('SplitRemainder === lit(SubFolder.Holdout.id), SubFolder.Holdout.id)
            .otherwise(SubFolder.Train.id))

      resultSet.cache()

      val subFolder = if (AudienceModelInputGeneratorConfig.subFolder != null) AudienceModelInputGeneratorConfig.subFolder else typePolicyTable._1._3.toString

      AudienceModelInputDataset(AudienceModelInputGeneratorConfig.model.toString, s"${typePolicyTable._1._1}_${typePolicyTable._1._2}").writePartition(
        resultSet.filter('SubFolder === lit(SubFolder.Val.id)).as[AudienceModelInputRecord],
        dateTime,
        subFolderKey = Some(subFolder),
        subFolderValue = Some(SubFolder.Val.toString),
        format = Some("tfrecord"),
        saveMode = SaveMode.Overwrite
      )

      if (AudienceModelInputGeneratorConfig.persistHoldoutSet) {
        AudienceModelInputDataset(AudienceModelInputGeneratorConfig.model.toString, s"${typePolicyTable._1._1}_${typePolicyTable._1._2}").writePartition(
          resultSet.filter('SubFolder === lit(SubFolder.Holdout.id)).as[AudienceModelInputRecord],
          dateTime,
          subFolderKey = Some(subFolder),
          subFolderValue = Some(SubFolder.Holdout.toString),
          format = Some("tfrecord"),
          saveMode = SaveMode.Overwrite
        )
      }

      AudienceModelInputDataset(AudienceModelInputGeneratorConfig.model.toString, s"${typePolicyTable._1._1}_${typePolicyTable._1._2}").writePartition(
        resultSet.filter('SubFolder === lit(SubFolder.Train.id)).as[AudienceModelInputRecord],
        dateTime,
        subFolderKey = Some(subFolder),
        subFolderValue = Some(SubFolder.Train.toString),
        format = Some("tfrecord"),
        saveMode = SaveMode.Overwrite
      )

      val calculateStats = udf((array: Seq[Float]) => {
        val totalElements = array.size
        val onesCount = array.sum
        val zerosCount = totalElements - onesCount

        (totalElements, onesCount, zerosCount)
      })

      val resultDF = resultSet.filter('SubFolder === lit(SubFolder.Train.id)).as[AudienceModelInputRecord]
        .withColumn("total", calculateStats(col("Targets")).getItem("_1"))
        .withColumn("pos", calculateStats(col("Targets")).getItem("_2"))
        .withColumn("neg", calculateStats(col("Targets")).getItem("_3"))
      resultDF.cache()
      val total = resultDF.select(sum("total")).cache().head().getLong(0)
      val pos_ratio = resultDF.select(sum("pos")).head().getDouble(0) / total
      MetadataDataset(AudienceModelInputGeneratorConfig.model).writeRecord(total, dateTime,"metadata", "Count")
      MetadataDataset(AudienceModelInputGeneratorConfig.model).writeRecord(pos_ratio, dateTime,"metadata", "PosRatio")
      posRatioGauge.labels(AudienceModelInputGeneratorConfig.model.toString.toLowerCase).set(pos_ratio)

      resultDF.unpersist()
    }
    )
  }

  def clusterTargetingData(model: Model.Value, supportedDataSources: Array[Int], seedSizeLowerScaleThreshold: Int, seedSizeUpperScaleThreshold: Int, schedule: Schedule.Schedule ):
  Map[(DataSource.DataSource, CrossDeviceVendor.CrossDeviceVendor, IncrementalTrainingTag.IncrementalTrainingTag), Array[AudienceModelPolicyRecord]] = {
    val policyTable = AudienceModelPolicyReadableDataset(model)
      .readSinglePartition(dateTime)(spark)
      .where((length('ActiveSize)>=seedSizeLowerScaleThreshold) && (length('ActiveSize)<seedSizeUpperScaleThreshold))
      // .where('SourceId==="1ufp35u0")
      .where('IsActive)
      .where('Source.isin(supportedDataSources: _*))
      .collect()

    // Handle incremental training schedule tag
    val processedPolicyTable = policyTable.map { record =>
      val tagValue = schedule match {
        case Schedule.Full => IncrementalTrainingTag.Full //
        case Schedule.Incremental => Try(record.Tag match {
          case 4 => IncrementalTrainingTag.Small
          case 2 | 6 => IncrementalTrainingTag.New
          case _ => IncrementalTrainingTag.None
        }).getOrElse(IncrementalTrainingTag.None)
        case _ => IncrementalTrainingTag.None
      }
      (record, tagValue)
    }

    processedPolicyTable.groupBy { case (e, tag) =>
      (DataSource(e.Source), CrossDeviceVendor(e.CrossDeviceVendorId), tag)
    }.mapValues(_.map(_._1))
  }
}
