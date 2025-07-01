package com.thetradedesk.audience.jobs.modelinput.rsmv2

import com.thetradedesk.audience.datasets._
import com.thetradedesk.audience.jobs.modelinput.rsmv2.BidImpSideDataGenerator.readBidImpressionWithNecessary
import com.thetradedesk.audience.jobs.modelinput.rsmv2.RSMV2SharedFunction.SubFolder
import com.thetradedesk.audience.jobs.modelinput.rsmv2.RelevanceModelInputGeneratorConfig._
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

object RelevanceModelInputGeneratorJob {
  val prometheus = new PrometheusClient("RelevanceModelJob", "RelevanceModelInputGeneratorJob")
  val jobRunningTime = prometheus.createGauge(s"relevance_etl_job_running_time", "RelevanceModelInputGeneratorJob running time", "model", "date")
  val jobProcessSize = prometheus.createGauge(s"relevance_etl_job_process_size", "RelevanceModelInputGeneratorJob process size", "model", "date", "data_source", "cross_device_vendor")

  def generateTrainingDataset(bidImpSideData: Dataset[BidSideDataRecord], seedLabelSideData: Dataset[SeedLabelSideDataRecord]) = {
    val res = bidImpSideData.join(seedLabelSideData, "BidRequestId")
      .select("TDID", "Site","Zip","BidRequestId","SplitRemainder","AdvertiserId","AliasedSupplyPublisherId",
        "Country","DeviceMake","DeviceModel","RequestLanguages","RenderingContext","DeviceType",
        "OperatingSystemFamily","Browser","Latitude","Longitude","Region","InternetConnectionType",
        "OperatingSystem","ZipSiteLevel_Seed","Targets","SyntheticIds","City","sin_hour_week","cos_hour_week",
        "sin_hour_day","cos_hour_day","sin_minute_hour","cos_minute_hour","sin_minute_day","cos_minute_day",
        "MatchedSegments", "MatchedSegmentsLength", "HasMatchedSegments", "UserSegmentCount", "IdTypesBitmap")

    val rawJson = readModelFeatures(rsmV2FeatureSourcePath)()

    if (rsmV2FeatureDestPath != null && rawJson != null) {
      FSUtils.writeStringToFile(rsmV2FeatureDestPath, rawJson)(spark)
    }

    // totalSplits default is 10 which train:val:holdout -> 8:1:1
    val totalSplits = trainValHoldoutTotalSplits
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
    val partitionsForValHoldout = math.min(math.max(1, rowCounts / minRowNumsPerPartition), audienceResultCoalesce).toInt
    val partitionsForTrain = math.min(math.max(1, (totalSplits - 2) * rowCounts / minRowNumsPerPartition), audienceResultCoalesce).toInt
    // TODO: remove hardcode seed_none
    RelevanceModelInputDataset(RelevanceModelInputGeneratorConfig.model.toString, "Seed_None").writePartition(
      resultSet.filter('SubFolder === lit(SubFolder.Val.id)).as[RelevanceModelInputRecord],
      dateTime,
      numPartitions = Some(partitionsForValHoldout),
      subFolderKey = Some(subFolder),
      subFolderValue = Some(SubFolder.Val.toString),
      format = Some("tfrecord"),
      saveMode = SaveMode.Overwrite
    )

    if (RelevanceModelInputGeneratorConfig.persistHoldoutSet) {
      RelevanceModelInputDataset(RelevanceModelInputGeneratorConfig.model.toString, "Seed_None").writePartition(
        resultSet.filter('SubFolder === lit(SubFolder.Holdout.id)).as[RelevanceModelInputRecord],
        dateTime,
        numPartitions = Some(partitionsForValHoldout),
        subFolderKey = Some(subFolder),
        subFolderValue = Some(SubFolder.Holdout.toString),
        format = Some("tfrecord"),
        saveMode = SaveMode.Overwrite
      )
    }

    RelevanceModelInputDataset(RelevanceModelInputGeneratorConfig.model.toString, "Seed_None").writePartition(
      resultSet.filter('SubFolder === lit(SubFolder.Train.id)).as[RelevanceModelInputRecord],
      dateTime,
      numPartitions = Some(partitionsForTrain),
      subFolderKey = Some(subFolder),
      subFolderValue = Some(SubFolder.Train.toString),
      format = Some("tfrecord"),
      saveMode = SaveMode.Overwrite
    )
  }

  def main(args: Array[String]): Unit = {
    val start = System.currentTimeMillis()
    val optInSeedGenerator = OptInSeedFactory.fromString(optInSeedType, optInSeedFilterExpr)
    val optInSeed = optInSeedGenerator.generate()
    val count = optInSeed.count()
    val rawBidReq = readBidImpressionWithNecessary()

    if (count > 0) {
      // prepare left
      val bidRes = BidImpSideDataGenerator.prepareBidImpSideFeatureDataset(rawBidReq)
      // prepare feature
      val userFs = if (useTmpFeatureGenerator) {
        UserZipSiteLevelFeatureGenerator.getFeature(bidRes.rawBidReqData, optInSeed)
      } else {
        UserZipSiteLevelFeatureExternalReader.getFeature(bidRes.rawBidReqData, optInSeed)
      }
      // prepare right
      val seedLabelSideData = PostExtraSamplingSeedLabelSideDataGenerator.prepareSeedSideFeatureAndLabel(optInSeed, bidRes.bidSideTrainingData, userFs)
      // join left and right
      generateTrainingDataset(bidRes.rawBidReqData, seedLabelSideData)
    } else {
      FSUtils.writeStringToFile(optInSeedEmptyTagPath, "")(spark)
    }

    jobRunningTime.labels(RelevanceModelInputGeneratorConfig.model.toString.toLowerCase, dateTime.toLocalDate.toString).set(System.currentTimeMillis() - start)
    prometheus.pushMetrics()
  }
  
}
