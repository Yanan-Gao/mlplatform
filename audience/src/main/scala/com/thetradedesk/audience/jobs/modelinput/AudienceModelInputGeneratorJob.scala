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
import org.apache.spark.sql.expressions.UserDefinedFunction
import org.apache.spark.sql.{DataFrame, SaveMode}
import org.apache.spark.sql.functions._

import java.time.LocalDateTime
import java.time.format.DateTimeFormatter
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
    val policyTable = clusterTargetingData(AudienceModelInputGeneratorConfig.model, AudienceModelInputGeneratorConfig.supportedDataSources, AudienceModelInputGeneratorConfig.supportedGraphs, AudienceModelInputGeneratorConfig.seedSizeUpperScaleThreshold, schedule)
    val countryMap = getSyntheticIdCountryMap(AudienceModelInputGeneratorConfig.model, AudienceModelInputGeneratorConfig.supportedDataSources, AudienceModelInputGeneratorConfig.seedSizeUpperScaleThreshold)

    val rawJson = readModelFeatures(featuresJsonSourcePath)()
    // write the rawJson to destination if present.
    if (featuresJsonDestPath != null && rawJson != null) {
      FSUtils.writeStringToFile(featuresJsonDestPath, rawJson)(spark)
    }

    val countryMapTransformed = ModelFeatureTransform
      .modelFeatureTransform[SyntheticIdToCountriesRecord](countryMap, rawJson)
      .select('Country, explode('CountryToSyntheticIds).alias("SyntheticId"))
      .groupBy('SyntheticId)
      .agg(collect_set('Country))
      .as[(Int, Seq[Int])]
      .collect()
      .map(e => e._1 -> e._2.toSet)
      .toMap

    val countryMapTransformedUDF = udf((syntheticIds: Seq[Int], targets: Seq[Float], country: Int) => {
      syntheticIds
        .zip(targets)
        .filter(e => countryMapTransformed(e._1).contains(country))
        .unzip
    })


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
                  case ((_, crossDeviceVendor: CrossDeviceVendor, IncrementalTrainingTag.Full), subPolicyTable: Array[AudienceModelPolicyRecord]) =>
                    RSMSeedInputGenerator(crossDeviceVendor, 1.0).generateDataset(date, subPolicyTable)
                  case _ => throw new Exception(s"unsupported policy settings: Model[${Model.RSM}], Setting[${typePolicyTable._1}]")
                }
              case Schedule.Incremental =>
                typePolicyTable match {
                  case ((_, crossDeviceVendor: CrossDeviceVendor, IncrementalTrainingTag.New), subPolicyTable: Array[AudienceModelPolicyRecord]) =>
                    RSMSeedInputGenerator(crossDeviceVendor, 1.0).generateDataset(date, subPolicyTable)
                  case ((_, crossDeviceVendor: CrossDeviceVendor, IncrementalTrainingTag.Small), subPolicyTable: Array[AudienceModelPolicyRecord]) =>
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

      val tempResultSet = ModelFeatureTransform.modelFeatureTransform[AudienceModelInputRecord](dataset, rawJson)
        .withColumn("SplitRemainder", xxhash64(concat('GroupId, lit(AudienceModelInputGeneratorConfig.saltToSplitDataset))) % AudienceModelInputGeneratorConfig.validateDatasetSplitModule)
        .withColumn("SubFolder",
          when('SplitRemainder === lit(SubFolder.Val.id), SubFolder.Val.id)
            .when('SplitRemainder === lit(SubFolder.Holdout.id), SubFolder.Holdout.id)
            .otherwise(SubFolder.Train.id))

        val resultSet = if (typePolicyTable._1._1 != DataSource.Seed) tempResultSet else
          tempResultSet
        .withColumn("f", countryMapTransformedUDF('SyntheticIds, 'Targets, 'Country))
        .withColumn("SyntheticIds", col("f._1"))
        .where(size('SyntheticIds) > 0)
        .withColumn("Targets", col("f._2"))
        .drop("f")

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

      // break down into FP and TP
      if (AudienceModelInputGeneratorConfig.breakDownSeedsByPermission && AudienceModelInputGeneratorConfig.model == Model.RSM && typePolicyTable._1._1 == DataSource.Seed && typePolicyTable._1._2 == CrossDeviceVendor.None) {
        separateThirdPartySeeds(dateTime, subFolder, SubFolder.Val.toString, typePolicyTable._2)
        separateThirdPartySeeds(dateTime, subFolder, SubFolder.Holdout.toString, typePolicyTable._2)
        separateThirdPartySeeds(dateTime, subFolder, SubFolder.Train.toString, typePolicyTable._2)
      }

      val calculateStats = udf((array: Seq[Float]) => {
        val totalElements = array.size
        val onesCount = array.sum.toInt
        val zerosCount = totalElements - onesCount

        (totalElements, onesCount, zerosCount)
      })

      val resultDF = resultSet.filter('SubFolder === lit(SubFolder.Train.id)).as[AudienceModelInputRecord]
        .withColumn("stats", calculateStats(col("Targets")))
        .withColumn("total", 'stats.getItem("_1"))
        .withColumn("pos", 'stats.getItem("_2"))
        .withColumn("neg", 'stats.getItem("_3"))
      resultDF.cache()
      // no data with new seeds
      val metric = if (typePolicyTable._1._3 == IncrementalTrainingTag.New && resultDF.count() == 0L) {
        Metric(0, 0, 0, 0d)
      } else {
        val metricDF = resultDF.select(sum("total"), sum("pos"), sum("neg")).cache()
        val total = metricDF.head().getLong(0)
        val pos = metricDF.head().getLong(1)
        val neg = metricDF.head().getLong(2)
        val pos_ratio = pos * 1.0 / total
        Metric(total, pos, neg, pos_ratio)
      }

      val currentBatch = s"${schedule}_${typePolicyTable._1._1}_${typePolicyTable._1._2}_${typePolicyTable._1._3}"
      MetadataDataset(AudienceModelInputGeneratorConfig.model).writeRecord(metric.total, dateTime, "metadata", s"Count_${currentBatch}")
      MetadataDataset(AudienceModelInputGeneratorConfig.model).writeRecord(metric.pos, dateTime, "metadata", s"Pos_${currentBatch}")
      MetadataDataset(AudienceModelInputGeneratorConfig.model).writeRecord(metric.neg , dateTime, "metadata", s"Neg_${currentBatch}")
      MetadataDataset(AudienceModelInputGeneratorConfig.model).writeRecord(metric.posRatio, dateTime, "metadata", s"PosRatio_${currentBatch}")
      posRatioGauge.labels(AudienceModelInputGeneratorConfig.model.toString.toLowerCase).set(metric.posRatio)

      resultDF.unpersist()
      resultSet.unpersist()
    }
    )
  }

  private def filterIdsUDF(filteredIds: Set[Long]) = udf(
    (ids: Seq[Long], targets: Seq[Float]) => {
      ids
        .zip(targets)
        .filter(e => filteredIds.contains(e._1))
        .unzip
    }
  )

  private def writeSeparateSeedData(df: DataFrame, syntheticIds: Set[Long], partitionedPath: String): Unit = {
    val output = df.withColumn("x", filterIdsUDF(syntheticIds)('SyntheticIds, 'Targets))
      .withColumn("SyntheticIds", col("x._1"))
      .withColumn("Targets", col("x._2"))
      .drop("x")
      .where(size('SyntheticIds) > lit(0))

    output.write.format("tfrecord")
      .option("recordType", "Example")
      .option("codec", "org.apache.hadoop.io.compress.GzipCodec")
      .save(partitionedPath)
  }

  private def separateThirdPartySeeds(dateTime: LocalDateTime, subFolderKey: String, subFolderValue: String, policyTable: Array[AudienceModelPolicyRecord]) {
    val dateStr = DateTimeFormatter.ofPattern(audienceVersionDateFormat).format(dateTime)
    val input = spark.read.format("tfrecord")
      .option("recordType", "Example")
      .load(s"s3://thetradedesk-mlplatform-us-east-1/data/${ttdEnv}/audience/${AudienceModelInputGeneratorConfig.model.toString}/Seed_None/v=1/${dateStr}/${subFolderKey}=${subFolderValue}")

    val fpSyntheticIds = policyTable.filter(e => e.PermissionTag == PermissionTag.Private.id).map(_.SyntheticId.toLong).toSet
    val tpSyntheticIds = policyTable.filter(e => e.PermissionTag == PermissionTag.Shared.id).map(_.SyntheticId.toLong).toSet

    writeSeparateSeedData(input, fpSyntheticIds, s"s3://thetradedesk-mlplatform-us-east-1/data/${ttdEnv}/audience/${AudienceModelInputGeneratorConfig.model.toString}/Seed_FP_None/v=1/${dateStr}/${subFolderKey}=${subFolderValue}")
    writeSeparateSeedData(input, tpSyntheticIds, s"s3://thetradedesk-mlplatform-us-east-1/data/${ttdEnv}/audience/${AudienceModelInputGeneratorConfig.model.toString}/Seed_TP_None/v=1/${dateStr}/${subFolderKey}=${subFolderValue}")
  }

  def getSyntheticIdCountryMap(model: Model.Value, supportedDataSources: Array[Int], seedSizeUpperScaleThreshold: Int) = {
    val countryMap = AudienceModelPolicyReadableDataset(model)
      .readSinglePartition(dateTime)(spark)
      .where(length('ActiveSize) <= seedSizeUpperScaleThreshold)
      .where('IsActive)
      .where('Source.isin(supportedDataSources: _*))
      .withColumn("Country",explode('topCountryByDensity))
      .groupBy("Country").agg(
        collect_set('SyntheticId).alias("CountryToSyntheticIds"))

    countryMap
  }

  def clusterTargetingData(model: Model.Value, supportedDataSources: Array[Int], supportedGraphs: Array[Int],seedSizeUpperScaleThreshold: Int, schedule: Schedule.Schedule):
  Map[(DataSource.DataSource, CrossDeviceVendor.CrossDeviceVendor, IncrementalTrainingTag.IncrementalTrainingTag), Array[AudienceModelPolicyRecord]] = {
    val policyTable = AudienceModelPolicyReadableDataset(model)
      .readSinglePartition(dateTime)(spark)
      .where(length('ActiveSize) < seedSizeUpperScaleThreshold)
      // .where('SourceId==="1ufp35u0")
      .where('IsActive)
      .where('Source.isin(supportedDataSources: _*))
      .where('CrossDeviceVendorId.isin(supportedGraphs: _*))
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

  case class Metric(total: Long, pos: Long, neg: Long, posRatio: Double)
}
