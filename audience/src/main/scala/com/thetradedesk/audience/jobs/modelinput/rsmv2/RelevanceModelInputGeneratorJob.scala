package com.thetradedesk.audience.jobs.modelinput.rsmv2

import com.thetradedesk.audience.datasets._
import com.thetradedesk.audience.jobs.modelinput.rsmv2.BidImpSideDataGenerator.readBidImpressionWithNecessary
import com.thetradedesk.audience.jobs.modelinput.rsmv2.RSMV2SharedFunction.SubFolder
import com.thetradedesk.confetti.AutoConfigResolvingETLJobBase

import java.time.{LocalDate, LocalDateTime}
import com.thetradedesk.audience.jobs.modelinput.rsmv2.datainterface.{BidSideDataRecord, SeedLabelSideDataRecord}
import com.thetradedesk.audience.jobs.modelinput.rsmv2.feature.userzipsite.{UserZipSiteLevelFeatureExternalReader, UserZipSiteLevelFeatureGenerator}
import com.thetradedesk.audience.jobs.modelinput.rsmv2.optinseed.OptInSeedFactory
import com.thetradedesk.audience.jobs.modelinput.rsmv2.seedlabelside.PostExtraSamplingSeedLabelSideDataGenerator
import com.thetradedesk.audience.transform.IDTransform.joinOnIdTypes
import com.thetradedesk.audience.{audienceResultCoalesce, dateTime}
import com.thetradedesk.geronimo.shared.readModelFeatures
import com.thetradedesk.geronimo.shared.transform.ModelFeatureTransform
import com.thetradedesk.spark.TTDSparkContext.spark
import com.thetradedesk.spark.TTDSparkContext.spark.implicits._
import com.thetradedesk.spark.util.io.FSUtils
import com.thetradedesk.spark.util.prometheus.PrometheusClient
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{DataFrame, Dataset, SaveMode}


case class RelevanceModelInputGeneratorJobConfig(
                                                  modelName: String,
                                                  useTmpFeatureGenerator: Boolean,
                                                  extraSamplingThreshold: Double,
                                                  rsmV2FeatureSourcePath: String,
                                                  rsmV2FeatureDestPath: String,
                                                  subFolder: String,
                                                  optInSeedEmptyTagPath: String,
                                                  densityFeatureReadPathWithoutSlash: String,
                                                  sensitiveFeatureColumns: Seq[String],
                                                  persistHoldoutSet: Boolean,
                                                  optInSeedType: String,
                                                  optInSeedFilterExpr: String,
                                                  posNegRatio: Int,
                                                  lowerLimitPosCntPerSeed: Int,
                                                  RSMV2UserSampleSalt: String,
                                                  RSMV2PopulationUserSampleIndex: Seq[Int],
                                                  RSMV2UserSampleRatio: Int,
                                                  samplerName: String,
                                                  overrideMode: Boolean,
                                                  splitRemainderHashSalt: String,
                                                  upLimitPosCntPerSeed: Int,
                                                  saveIntermediateResult: Boolean,
                                                  intermediateResultBasePathEndWithoutSlash: String,
                                                  maxLabelLengthPerRow: Int,
                                                  minRowNumsPerPartition: Int,
                                                  trainValHoldoutTotalSplits: Int,
                                                  activeSeedIdWhiteList: String,
                                                  runDate: LocalDate
                                                )


object RelevanceModelInputGeneratorJob
  extends AutoConfigResolvingETLJobBase[RelevanceModelInputGeneratorJobConfig](
    groupName = "audience",
    jobName = "RelevanceModelInputGeneratorJob") {
  override val prometheus: Option[PrometheusClient] =
    Some(new PrometheusClient("RelevanceModelJob", "RelevanceModelInputGeneratorJob"))
  val jobRunningTime = prometheus.get.createGauge(s"relevance_etl_job_running_time", "RelevanceModelInputGeneratorJob running time", "model", "date")
  val jobProcessSize = prometheus.get.createGauge(s"relevance_etl_job_process_size", "RelevanceModelInputGeneratorJob process size", "model", "date", "data_source", "cross_device_vendor")

  var conf: RelevanceModelInputGeneratorJobConfig = _

  def generateTrainingDataset(bidImpSideData: Dataset[BidSideDataRecord], seedLabelSideData: Dataset[SeedLabelSideDataRecord]) = {
    val res = bidImpSideData.join(seedLabelSideData, "BidRequestId")
      .select("TDID", "Site","Zip","BidRequestId","SplitRemainder","AdvertiserId","AliasedSupplyPublisherId",
        "Country","DeviceMake","DeviceModel","RequestLanguages","RenderingContext","DeviceType",
        "OperatingSystemFamily","Browser","Latitude","Longitude","Region","InternetConnectionType",
        "OperatingSystem","ZipSiteLevel_Seed","Targets","SyntheticIds","City","sin_hour_week","cos_hour_week",
        "sin_hour_day","cos_hour_day","sin_minute_hour","cos_minute_hour","sin_minute_day","cos_minute_day",
        "MatchedSegments", "MatchedSegmentsLength", "HasMatchedSegments", "UserSegmentCount", "IdTypesBitmap")

    val rawJson = readModelFeatures(conf.rsmV2FeatureSourcePath)()

    if (conf.rsmV2FeatureDestPath != null && rawJson != null) {
      FSUtils.writeStringToFile(conf.rsmV2FeatureDestPath, rawJson)(spark)
    }

    // totalSplits default is 10 which train:val:holdout -> 8:1:1
    val totalSplits = conf.trainValHoldoutTotalSplits
    val resultSet = ModelFeatureTransform.modelFeatureTransform[RelevanceModelInputRecord](res, rawJson)
      .withColumn("SubFolder",
        when('SplitRemainder === lit(SubFolder.Val.id), SubFolder.Val.id)
          .when('SplitRemainder === lit(SubFolder.Holdout.id), SubFolder.Holdout.id)
          .otherwise(SubFolder.Train.id))
      .withColumn("rand", rand())
      .orderBy("rand").drop("rand")
      .cache()

    // ensure one file at least 30k rows to make model learn well
    val rowCounts = res.count() / totalSplits
    val partitionsForValHoldout = math.min(math.max(1, rowCounts / conf.minRowNumsPerPartition), audienceResultCoalesce).toInt
    val partitionsForTrain = math.min(math.max(1, (totalSplits - 2) * rowCounts / conf.minRowNumsPerPartition), audienceResultCoalesce).toInt
    // TODO: remove hardcode seed_none
    RelevanceModelInputDataset(conf.modelName, "Seed_None").writePartition(
      resultSet.filter('SubFolder === lit(SubFolder.Val.id)).as[RelevanceModelInputRecord],
      dateTime,
      numPartitions = Some(partitionsForValHoldout),
      subFolderKey = Some(conf.subFolder),
      subFolderValue = Some(SubFolder.Val.toString),
      format = Some("tfrecord"),
      saveMode = SaveMode.Overwrite
    )

    if (conf.persistHoldoutSet) {
      RelevanceModelInputDataset(conf.modelName, "Seed_None").writePartition(
        resultSet.filter('SubFolder === lit(SubFolder.Holdout.id)).as[RelevanceModelInputRecord],
        dateTime,
        numPartitions = Some(partitionsForValHoldout),
        subFolderKey = Some(conf.subFolder),
        subFolderValue = Some(SubFolder.Holdout.toString),
        format = Some("tfrecord"),
        saveMode = SaveMode.Overwrite
      )
    }

    RelevanceModelInputDataset(conf.modelName, "Seed_None").writePartition(
      resultSet.filter('SubFolder === lit(SubFolder.Train.id)).as[RelevanceModelInputRecord],
      dateTime,
      numPartitions = Some(partitionsForTrain),
      subFolderKey = Some(conf.subFolder),
      subFolderValue = Some(SubFolder.Train.toString),
      format = Some("tfrecord"),
      saveMode = SaveMode.Overwrite
    )
  }

  override def runETLPipeline(): Unit = {
    conf = getConfig
    dateTime = conf.runDate.atStartOfDay()

    val start = System.currentTimeMillis()
    val optInSeedGenerator = OptInSeedFactory.fromString(conf.optInSeedType, conf.optInSeedFilterExpr)
    val optInSeed = optInSeedGenerator.generate(conf)
    val count = optInSeed.count()
    val rawBidReq = readBidImpressionWithNecessary(conf)

    if (count > 0) {
      // prepare left
      val bidRes = BidImpSideDataGenerator.prepareBidImpSideFeatureDataset(rawBidReq, conf)
      // prepare feature
      val userFs = if (conf.useTmpFeatureGenerator) {
        UserZipSiteLevelFeatureGenerator.getFeature(bidRes.rawBidReqData, optInSeed, conf)
      } else {
        UserZipSiteLevelFeatureExternalReader.getFeature(bidRes.rawBidReqData, optInSeed, conf)
      }
      // prepare right
      val seedLabelSideData = PostExtraSamplingSeedLabelSideDataGenerator.prepareSeedSideFeatureAndLabel(optInSeed, bidRes.bidSideTrainingData, userFs, conf)
      // join left and right
      generateTrainingDataset(bidRes.rawBidReqData, seedLabelSideData)
    } else {
      FSUtils.writeStringToFile(conf.optInSeedEmptyTagPath, "")(spark)
    }

    jobRunningTime.labels(conf.modelName.toLowerCase, dateTime.toLocalDate.toString).set(System.currentTimeMillis() - start)
  }
}
