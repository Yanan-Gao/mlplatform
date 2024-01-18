package com.thetradedesk.audience.jobs

import com.thetradedesk.audience.datasets._
import com.thetradedesk.audience.featuresJsonPath
import com.thetradedesk.geronimo.shared.transform.ModelFeatureTransform
import com.thetradedesk.audience.utils.OnlineLogsParser
import com.thetradedesk.geronimo.bidsimpression.schema.{BidsImpressions, BidsImpressionsSchema}
import com.thetradedesk.geronimo.shared.schemas.{BidRequestDataset, GeronimoBidRequestRecord}
import com.thetradedesk.geronimo.shared.{GERONIMO_DATA_SOURCE, loadParquetData}
import com.thetradedesk.spark.TTDSparkContext.spark
import com.thetradedesk.spark.TTDSparkContext.spark.implicits._
import com.thetradedesk.spark.util.TTDConfig.config
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{DataFrame, Dataset, SaveMode, SparkSession}

import java.time.LocalDate

abstract class AudienceModelMonitoringDataGenerator {
  object Config {
    val date: LocalDate = config.getDate("date", LocalDate.now())

    val model: Model.Value = Model.withName(config.getStringRequired("modelName"))

    val modelS3Path: String = config.getString("modelS3Path", default="")

    val bidsImpressionLookBack: Int = config.getInt("bidsImpressionLookBack", 0)

    val datasetName: String = config.getString("datasetName", "offlineMonitoring")

    val datasetVersion: Int = config.getInt("datasetVersion", 1)
  }

  def getUnsampledBidImpressions(date: LocalDate): DataFrame = {
    val bidImpressionsS3Path = BidsImpressions.BIDSIMPRESSIONSS3 + "prod/bidsimpressions/"
    val bidsImpressions = loadParquetData[BidsImpressionsSchema](bidImpressionsS3Path, date, source = Some(GERONIMO_DATA_SOURCE), lookBack = Some(Config.bidsImpressionLookBack))
      .withColumnRenamed("UIID", "TDID")
      .select('BidRequestId, // use to connect with bidrequest, to get more features
        'AdvertiserId,
        'AdGroupId,
        'SupplyVendor,
        'DealId,
        'SupplyVendorPublisherId,
        'Site,
        'AdWidthInPixels,
        'AdHeightInPixels,
        'Country,
        'Region,
        'City,
        'Zip, // cast to first three digits for US is enough
        'DeviceMake,
        'DeviceModel,
        'RequestLanguages,
        'RenderingContext,
        'DeviceType,
        'OperatingSystemFamily,
        'Browser,
        'sin_hour_week, // time based features sometime are useful than expected
        'cos_hour_week,
        'sin_hour_day,
        'cos_hour_day,
        'Latitude,
        'Longitude,
        'MatchedFoldPosition,
        'InternetConnectionType,
        'OperatingSystem,
        'sin_minute_hour,
        'cos_minute_hour,
        'sin_minute_day,
        'cos_minute_day
      )
      // they saved in struct type
      .withColumn("OperatingSystemFamily", 'OperatingSystemFamily("value"))
      .withColumn("Browser", 'Browser("value"))
      .withColumn("RenderingContext", 'RenderingContext("value"))
      .withColumn("InternetConnectionType", 'InternetConnectionType("value"))
      .withColumn("OperatingSystem", 'OperatingSystem("value"))
      .withColumn("DeviceType", 'DeviceType("value"))
      .withColumn("AdWidthInPixels", ('AdWidthInPixels - lit(1.0)) / lit(9999.0)) // 1 - 10000
      .withColumn("AdWidthInPixels", when('AdWidthInPixels.isNotNull, 'AdWidthInPixels).otherwise(0))
      .withColumn("AdHeightInPixels", ('AdHeightInPixels - lit(1.0)) / lit(9999.0)) // 1 - 10000
      .withColumn("AdHeightInPixels", when('AdHeightInPixels.isNotNull, 'AdHeightInPixels).otherwise(0))
      .withColumn("Latitude", ('Latitude + lit(90.0)) / lit(180.0)) // -90 - 90
      .withColumn("Latitude", when('Latitude.isNotNull, 'Latitude).otherwise(0))
      .withColumn("Longitude", ('Longitude + lit(180.0)) / lit(360.0)) //-180 - 180
      .withColumn("Longitude", when('Longitude.isNotNull, 'Longitude).otherwise(0))

    bidsImpressions
  }

  def getBidRequests(date: LocalDate): DataFrame = {
    loadParquetData[GeronimoBidRequestRecord](s3path = BidRequestDataset.BIDSS3, date = date)
      .select('AvailableBidRequestId, 'BidRequestId)
  }

  def getOnlineLogs(date: LocalDate, modelName: String): DataFrame = {
    implicit val spark: SparkSession = SparkSession.builder().getOrCreate()
    OnlineLogsDataset(modelName).readPartition(date).toDF
  }

  def getFeaturesSchema(onlineLogs: DataFrame, modelS3Path: String): String = {
    // if model path not specified, use features schema corresponding to the latest model version
    // otherwise, use the corresponding features schema
    // TODO: add support for monitoring different versions simultaneously
    val featuresVersion = if (modelS3Path == "") {
      onlineLogs.orderBy(desc("ModelVersion"))
        .select('ModelVersion, 'FeaturesVersion)
        .take(1)(0)(1)
        .toString
    } else {
      // strip prefix off the path to match the log format
      val modelVersion = List("s3://", "s3a://").foldLeft(modelS3Path) { (str, prefix) =>
        str.stripPrefix(prefix)
      }
      onlineLogs.select('ModelVersion, 'FeaturesVersion)
        .where('ModelVersion === modelVersion)
        .take(1)(0)(1)
        .toString
    }

    val featuresSchemaS3Path = "s3a://" + featuresVersion

    spark.read.option("multiLine", value = true).json(featuresSchemaS3Path).toJSON.take(1)(0)
  }

  def runETLPipeline(date: LocalDate, modelName: String, modelS3Path: String): (Dataset[AudienceModelMonitoringRecord], DataFrame)
}

object RSMMonitoringDataGenerator extends AudienceModelMonitoringDataGenerator {
  def parseOnlineLogs(dfOnlineLogs: DataFrame, featuresSchema: String): DataFrame = {
    val featureNames = OnlineLogsParser.extractFeatureNames(featuresSchema)
    val dfParsedJson = dfOnlineLogs.withColumn("ParsedJson", OnlineLogsParser.parseFeatureJsonUDF('Features))
    val dfOnlineLogsParsed = featureNames.foldLeft(dfParsedJson)((df, f) => df.withColumn(f, OnlineLogsParser.extractFeatureUDF('ParsedJson, lit(f))))
      .withColumn("SyntheticIdsRaw", explode(OnlineLogsParser.extractDynamicFeatureUDF('DynamicFeatures, lit("SyntheticId"))))
      .drop("Features", "DynamicFeatures", "ParsedJson")

    // filter out all rows in which OnlineModelScore or any feature value is NULL
    // since null checks are already implemented in the form of online monitoring alarms, no need to investigate offline
    val filterNullStr = featureNames.foldLeft("OnlineModelScore is NULL")((str, f) =>
      str + s" OR $f is NULL"
    )
    val nullRecords = dfOnlineLogsParsed.filter(filterNullStr)

    if (nullRecords.isEmpty) dfOnlineLogsParsed else dfOnlineLogsParsed.na.drop()
  }

  override def runETLPipeline(date: LocalDate, modelName: String, modelS3Path: String): (Dataset[AudienceModelMonitoringRecord], DataFrame) = {
    val bidImpressions = getUnsampledBidImpressions(date)
    val onlineLogs = getOnlineLogs(date, modelName)
    val featuresSchema = getFeaturesSchema(onlineLogs, modelS3Path)
    val parsedOnlineLogs = parseOnlineLogs(onlineLogs, featuresSchema)
    val bidRequests = getBidRequests(date)

    // start from logs, join w/ bid requests to get AvailId -> BidReqId mapping and finally join w/ bid imps
    val monitoringData = parsedOnlineLogs.select('AvailableBidRequestId, 'OnlineModelScore, 'SyntheticIdsRaw)
      .withColumnRenamed("SyntheticIdsRaw", "SyntheticIds")
      .withColumn("OnlineModelScore", col("OnlineModelScore").cast("double"))
      .join(bidRequests, Seq("AvailableBidRequestId"), "inner")
      .join(bidImpressions, Seq("BidRequestId"), "inner")

    val dataset = ModelFeatureTransform.modelFeatureTransform[AudienceModelMonitoringRecord](monitoringData, featuresJsonPath)

    (dataset, parsedOnlineLogs)
  }

  def main(args: Array[String]): Unit = {
    val (dataset, onlineLogs) = runETLPipeline(Config.date, Config.model.toString, Config.modelS3Path)

    // TODO: perform feature comparison b/w onlineLogs and dataset using except and union

    // TODO: sample the dataset before writing or before model evaluation?

    // parquet format for feature and output comparison
    AudienceModelMonitoringDataset(Config.datasetName, Config.datasetVersion).writePartition(
      dataset = dataset,
      Config.date,
      subFolderKey = Option("format"),
      subFolderValue = Some("parquet"),
      format = Some("parquet"),
      saveMode = SaveMode.Overwrite
    )

    // tfrecord format for model evaluation
    AudienceModelMonitoringDataset("offlineMonitoring").writePartition(
      dataset = dataset,
      Config.date,
      subFolderKey = Option("format"),
      subFolderValue = Some("tfrecord"),
      format = Some("tfrecord"),
      saveMode = SaveMode.Overwrite
    )
  }
}

// TODO: to be refactored later when there's a need for AEM monitoring
//object FirstPartyPixelMonitoringDataGenerator extends AudienceModelMonitoringDataGenerator {
//  def parseOnlineLogs(dfOnlineLogs: DataFrame, featuresSchema: String): DataFrame = {
//    val featureNames = OnlineLogsParser.extractFeatureNames(featuresSchema)
//
//    val dfParsedJson = dfOnlineLogs.withColumn("ParsedJson", OnlineLogsParser.parseFeatureJsonUDF('Features))
//
//    val dfOnlineLogsParsed = featureNames.foldLeft(dfParsedJson)((df, f) => df.withColumn(f, OnlineLogsParser.extractFeatureUDF('ParsedJson, lit(f))))
//      .withColumn("TargetingDataIdRaw", explode(OnlineLogsParser.extractDynamicFeatureUDF('DynamicFeatures, lit("TargetingDataId"))))
//      .drop("Features", "DynamicFeatures", "ParsedJson")
//
//    // filter out all rows in which OnlineModelScore or any feature value is NULL
//    val filterNullStr = featureNames.foldLeft(s"OnlineModelScore is NULL")((str, f) =>
//        str + s" OR $f is NULL"
//      )
//
//    val nullRecords = dfOnlineLogsParsed.filter(filterNullStr)
//
//    // TODO: write null records (if any) to S3 for further investigation (?)
//    if (nullRecords.isEmpty) dfOnlineLogsParsed else dfOnlineLogsParsed.na.drop()
//  }
//
//  def runETLPipeline(date: LocalDate, modelName: String, modelS3Path: String): (Dataset[AudienceModelMonitoringRecord], DataFrame) = {
//    val bidImpressions = getUnsampledBidImpressions(date)
//    val onlineLogs = getOnlineLogs(date, modelName)
//    val featuresSchema = getFeaturesSchema(onlineLogs, modelS3Path)
//    val parsedOnlineLogs = parseOnlineLogs(onlineLogs, featuresSchema)
//    val bidRequests = getBidRequests(date)
//
//    // start from logs, join w/ bid requests to get AvailId -> BidReqId mapping and finally join w/ bid imps
//    val monitoringData = parsedOnlineLogs.select('AvailableBidRequestId, 'OnlineModelScore, 'TargetingDataIdRaw)
//      .withColumnRenamed("TargetingDataIdRaw", "TargetingDataId")
//      .withColumn("OnlineModelScore", col("OnlineModelScore").cast("double"))
//      .join(bidRequests, Seq("AvailableBidRequestId"), "inner")
//      .join(bidImpressions, Seq("BidRequestId"), "inner")
//
//    val offlineMonitoringDataset = ModelFeatureTransform.modelFeatureTransform[AudienceModelMonitoringRecord](monitoringData)
//
//    (offlineMonitoringDataset, parsedOnlineLogs)
//  }
//
//  def main(args: Array[String]): Unit = {
//    val (dataset, onlineLogs) = runETLPipeline(Config.date, Config.model.toString, Config.modelS3Path)
//
//    // TODO: perform feature comparison b/w onlineLogs and dataset using except and union
//
//    // TODO: sample the dataset before writing or before model evaluation?
//
//    // parquet format for feature and output comparison
//    AudienceModelMonitoringDataset(Config.datasetName, Config.datasetVersion).writePartition(
//      dataset = dataset,
//      Config.date,
//      subFolderKey = Option("format"),
//      subFolderValue = Some("parquet"),
//      format = Some("parquet"),
//      saveMode = SaveMode.Overwrite
//    )
//
//    // tfrecord format for model evaluation
//    AudienceModelMonitoringDataset("offlineMonitoring", 1).writePartition(
//      dataset = dataset,
//      Config.date,
//      subFolderKey = Option("format"),
//      subFolderValue = Some("tfrecord"),
//      format = Some("tfrecord"),
//      saveMode = SaveMode.Overwrite
//    )
//  }
//}