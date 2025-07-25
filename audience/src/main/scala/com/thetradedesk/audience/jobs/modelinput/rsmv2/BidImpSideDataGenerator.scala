package com.thetradedesk.audience.jobs.modelinput.rsmv2

import com.thetradedesk.audience.{date, getUiid}
import com.thetradedesk.audience.jobs.modelinput.rsmv2.datainterface.{BidResult, BidSideDataRecord}
import com.thetradedesk.audience.jobs.modelinput.rsmv2.usersampling.SamplerFactory
import com.thetradedesk.audience.transform.IDTransform.{allIdWithType, filterOnIdTypes, filterOnIdTypesSym, idTypesBitmap}
import com.thetradedesk.geronimo.bidsimpression.schema.{BidsImpressions, BidsImpressionsSchema}
import com.thetradedesk.geronimo.shared.{GERONIMO_DATA_SOURCE, loadParquetData}
import com.thetradedesk.spark.TTDSparkContext.spark.implicits._
import org.apache.spark.sql.{DataFrame, Dataset}
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._

object BidImpSideDataGenerator {

  def readBidImpressionWithNecessary(conf: RelevanceModelInputGeneratorJobConfig): Dataset[BidSideDataRecord] = {
    val sampler = SamplerFactory.fromString(conf.samplerName, conf)
    val bidImpressionsS3Path = BidsImpressions.BIDSIMPRESSIONSS3 + "prod/bidsimpressions/"

    val bidsImpressionsLongRaw = loadParquetData[BidsImpressionsSchema](bidImpressionsS3Path, date, lookBack = Some(0), source = Some(GERONIMO_DATA_SOURCE))
      .filter(filterOnIdTypesSym(sampler.samplingFunction))
      .withColumn("TDID", getUiid('UIID, 'UnifiedId2, 'EUID, 'IdentityLinkId, 'IdType))
      .select(
          "TDID",
          "Site",
          "Zip",
          "DeviceAdvertisingId",
          "CookieTDID",
          "UnifiedId2",
          "EUID",
          "IdentityLinkId",
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
      .withColumn("SplitRemainderUserId", coalesce('DeviceAdvertisingId, 'CookieTDID, 'UnifiedId2, 'EUID, 'IdentityLinkId))
      .withColumn("SplitRemainder", (abs(xxhash64(concat('SplitRemainderUserId, lit(conf.splitRemainderHashSalt)))) % conf.trainValHoldoutTotalSplits).cast("int"))
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
      .withColumn("HasMatchedSegments", when('MatchedSegmentsLength > lit(0), 1).otherwise(0))
      .withColumn("UserSegmentCount", when('UserSegmentCount.isNull, 0.0).otherwise('UserSegmentCount.cast(DoubleType)))
      .withColumn("IdTypesBitmap", idTypesBitmap)

    // mask sensitive
    val bidsImpressionsLong = conf.sensitiveFeatureColumns.foldLeft(bidsImpressionsLongRaw) { (currentDs, colName) =>
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
    RSMV2SharedFunction.writeOrCache(Option(writePath), conf.overrideMode, bidsImpressionsLong, cache=false).as[BidSideDataRecord]
  }

  def prepareBidImpSideFeatureDataset(rawBidReq: Dataset[BidSideDataRecord], conf: RelevanceModelInputGeneratorJobConfig): BidResult = {

    val dateStr = RSMV2SharedFunction.getDateStr()

    // one TDID random keep one record
    // TODO:  how big of a change in model perf it will make if we have more samples per TDID
    val res = rawBidReq
      .withColumn("rand", rand())
      .withColumn("AllIdsWithIdTypes", allIdWithType)
      .withColumn("TDID", col("AllIdsWithIdTypes").getField("_1"))
      .withColumn("row", row_number().over(Window.partitionBy("TDID").orderBy("rand")))
      .filter(col("row") === 1)
      .drop("rand", "row", "AllIdsWithIdTypes")

    var writePath: String = null
    if (conf.saveIntermediateResult) {
      writePath = s"s3://${conf.intermediateResultBasePathEndWithoutSlash}/${dateStr}/all_bidreq_features"
    }

    BidResult(rawBidReq, RSMV2SharedFunction.writeOrCache(Option(writePath), conf.overrideMode, res).as[BidSideDataRecord])
  }

}
