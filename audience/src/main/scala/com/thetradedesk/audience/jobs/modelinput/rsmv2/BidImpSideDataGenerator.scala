package com.thetradedesk.audience.jobs.modelinput.rsmv2

import com.thetradedesk.audience.{date, getUiid}
import com.thetradedesk.audience.jobs.modelinput.rsmv2.datainterface.{BidResult, BidSideDataRecord}
import com.thetradedesk.audience.jobs.modelinput.rsmv2.usersampling.SamplerFactory
import com.thetradedesk.geronimo.bidsimpression.schema.{BidsImpressions, BidsImpressionsSchema}
import com.thetradedesk.geronimo.shared.{GERONIMO_DATA_SOURCE, loadParquetData}
import com.thetradedesk.spark.TTDSparkContext.spark.implicits._
import org.apache.spark.sql.{DataFrame, Dataset}
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._

object BidImpSideDataGenerator {

  def readBidImpressionWithNecessary(conf: RelevanceModelInputGeneratorJobConfig): Dataset[BidSideDataRecord] = {
    val sampler = SamplerFactory.fromString(conf.samplerName)
    val bidImpressionsS3Path = BidsImpressions.BIDSIMPRESSIONSS3 + "prod/bidsimpressions/"

    val bidsImpressionsLongRaw = loadParquetData[BidsImpressionsSchema](bidImpressionsS3Path, date, lookBack = Some(0), source = Some(GERONIMO_DATA_SOURCE))
      .withColumn("TDID", getUiid('UIID, 'UnifiedId2, 'EUID, 'IdType))
      .filter('TDID.isNotNull && 'TDID =!= lit("00000000-0000-0000-0000-000000000000"))
      .filter(sampler.samplingFunction('TDID, conf))
      .select(
          "Site",
          "Zip",
          "TDID",
          "BidRequestId",
          "AdvertiserId",
          "AliasedSupplyPublisherId",
          "Country",
          "DeviceMake",
          "DeviceModel",
          "RequestLanguages",
          "RenderingContext",
          "DeviceType",
          "OperatingSystemFamily",
          "Browser",
          "Latitude",
          "Longitude",
          "Region",
          "City",
          "InternetConnectionType",
          "OperatingSystem",
          "sin_hour_week",
          "cos_hour_week",
          "sin_hour_day",
          "cos_hour_day",
          "sin_minute_hour",
          "cos_minute_hour",
          "sin_minute_day",
          "cos_minute_day",
          "MatchedSegments",
          "UserSegmentCount"
      )
      .withColumn("OperatingSystemFamily", 'OperatingSystemFamily("value"))
      .withColumn("Browser", 'Browser("value"))
      .withColumn("RenderingContext", 'RenderingContext("value"))
      .withColumn("InternetConnectionType", 'InternetConnectionType("value"))
      .withColumn("OperatingSystem", 'OperatingSystem("value"))
      .withColumn("DeviceType", 'DeviceType("value"))
      .withColumn("Latitude", ('Latitude + lit(90.0)) / lit(180.0)) // -90 - 90
      .withColumn("Latitude", when('Latitude.isNotNull, 'Latitude).otherwise(0))
      .withColumn("Longitude", ('Longitude + lit(180.0)) / lit(360.0)) //-180 - 180
      .withColumn("Longitude", when('Longitude.isNotNull, 'Longitude).otherwise(0))
      .withColumn("MatchedSegmentsLength", when('MatchedSegments.isNull,0.0).otherwise(size('MatchedSegments).cast(DoubleType)))
      .withColumn("HasMatchedSegments", when('MatchedSegments.isNull,0).otherwise(1))
      .withColumn("UserSegmentCount", when('UserSegmentCount.isNull, 0.0).otherwise('UserSegmentCount.cast(DoubleType)))

    // mask sensitive
    val bidsImpressionsLong = conf.sensitiveFeatureColumn.split(",").map(_.trim).filter(_.nonEmpty).foldLeft(bidsImpressionsLongRaw) { (currentDs, colName) =>
      if (currentDs.columns.contains(colName))
        currentDs.withColumn(colName, lit(0))
      else
        currentDs
    }

    val dateStr = RSMV2SharedFunction.getDateStr()
    var writePath: String = null
    if (conf.saveIntermediateResult) {
      writePath = s"s3://${conf.intermediateResultBasePathEndWithoutSlash}/${dateStr}/bidreq"
    }
    RSMV2SharedFunction.writeOrCache(Option(writePath), conf.overrideMode, bidsImpressionsLong).as[BidSideDataRecord]
  }

  def prepareBidImpSideFeatureDataset(conf: RelevanceModelInputGeneratorJobConfig): BidResult = {

    val dateStr = RSMV2SharedFunction.getDateStr()
    val rawBidReq = readBidImpressionWithNecessary(conf)

    // one TDID random keep one record
    // TODO:  how big of a change in model perf it will make if we have more samples per TDID
    val res = rawBidReq
      .withColumn("rand", rand())
      .withColumn("row", row_number().over(Window.partitionBy("TDID").orderBy("rand")))
      .filter(col("row") === 1)
      .drop("rand", "row")

    var writePath: String = null
    if (conf.saveIntermediateResult) {
      writePath = s"s3://${conf.intermediateResultBasePathEndWithoutSlash}/${dateStr}/all_bidreq_features"
    }

    BidResult(rawBidReq, RSMV2SharedFunction.writeOrCache(Option(writePath), conf.overrideMode, res).as[BidSideDataRecord])
  }

}
