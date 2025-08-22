package com.thetradedesk.audience.jobs
import com.thetradedesk.audience.datasets.{RelevanceModelInputDataset, RelevanceModelInputRecord, Model}
import com.thetradedesk.audience.{audienceVersionDateFormat, date, dateTime, doNotTrackTDID, ttdEnv}
import com.thetradedesk.confetti.AutoConfigResolvingETLJobBase
import com.thetradedesk.audience.jobs.modelinput.rsmv2.usersampling.SIBSampler.{_isDeviceIdSampled1Percent, _isDeviceIdSampledNPercent}
import com.thetradedesk.audience.transform.IDTransform
import com.thetradedesk.geronimo.bidsimpression.schema.{BidsImpressions, BidsImpressionsSchema}
import com.thetradedesk.geronimo.shared.transform.ModelFeatureTransform
import com.thetradedesk.audience.jobs.modelinput.rsmv2.RSMV2SharedFunction.{paddingColumns, castDoublesToFloat}
import com.thetradedesk.geronimo.shared.{GERONIMO_DATA_SOURCE, loadParquetData, readModelFeatures}
import com.thetradedesk.spark.TTDSparkContext.spark.implicits._
import com.thetradedesk.spark.util.prometheus.PrometheusClient
import com.thetradedesk.confetti.utils.Logger
import org.apache.spark.sql.SaveMode
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.FloatType
import com.thetradedesk.spark.util.TTDConfig.config
import com.thetradedesk.audience.jobs.modelinput.rsmv2.RelevanceModelInputGeneratorConfig
import com.thetradedesk.audience.transform.IDTransform.{allIdWithType, filterOnIdTypes, filterOnIdTypesSym, idTypesBitmap}
import com.thetradedesk.audience.utils.IDTransformUtils.addGuidBits
import org.apache.spark.sql.expressions.UserDefinedFunction
import java.time.format.DateTimeFormatter
import java.time.LocalDate
import scala.collection.mutable.ArrayBuffer

case class Imp2BrModelInferenceDataGeneratorConfig(
  feature_path: String,
  runDate: LocalDate,
  samplingRate: Int
)

class Imp2BrModelInferenceDataGenerator(prometheus: PrometheusClient) {

  val bidImpressionsS3Path: String = BidsImpressions.BIDSIMPRESSIONSS3 + "prod/bidsimpressions/"

  private def makeSampleFun(n: Int): String => Boolean = {
     (id: String) => {
       if (id == null || id.isEmpty) false
       else _isDeviceIdSampledNPercent(id, n)
       }
  }

  def getAllUiidsUdfWithSample(sampleFun: String => Boolean) = udf((tdid: String, deviceAdvertisingId: String, uid2: String, euid: String, identityLinkId: String) => {
    val uiids = ArrayBuffer[(String, Int)]()

    // when CookieTDID == DeviceAdvertisingId, keep the latter
    // we don't expect clash among other id types
    if (tdid != null && tdid != doNotTrackTDID && sampleFun(tdid) && tdid != deviceAdvertisingId) {
      uiids.append((tdid, IDTransform.IDType.CookieTDID.id))
    }
    if (deviceAdvertisingId != null && deviceAdvertisingId != doNotTrackTDID && sampleFun(deviceAdvertisingId)) {
      uiids.append((deviceAdvertisingId, IDTransform.IDType.DeviceAdvertisingId.id))
    }
    if (uid2 != null && uid2 != doNotTrackTDID && sampleFun(uid2)) {
      uiids.append((uid2, IDTransform.IDType.UnifiedId2.id))
    }
    if (euid != null && euid != doNotTrackTDID && sampleFun(euid)) {
      uiids.append((euid, IDTransform.IDType.EUID.id))
    }
    if (identityLinkId != null && identityLinkId != doNotTrackTDID && sampleFun(identityLinkId)) {
      uiids.append((identityLinkId, IDTransform.IDType.IdentityLinkId.id))
    }
    uiids.toSeq
  })

  def run(conf: Imp2BrModelInferenceDataGeneratorConfig, logger: Logger): Unit = {
    val date = conf.runDate
    val dateTime = conf.runDate.atStartOfDay()

    val bidsImpressions = loadParquetData[BidsImpressionsSchema](bidImpressionsS3Path, date, lookBack=Some(0), source = Some(GERONIMO_DATA_SOURCE))
      .withColumn("Uiids", getAllUiidsUdfWithSample(makeSampleFun( conf.samplingRate ))('CookieTDID, 'DeviceAdvertisingId, 'UnifiedId2, 'EUID, 'IdentityLinkId))
      .withColumn("TdidAndIdType", explode(col("Uiids")))
      .withColumn("TDID", col("TdidAndIdType._1"))
      .withColumn("IDType", col("TdidAndIdType._2"))
      .drop("Uiids", "TdidAndIdType")
      .filter("SUBSTRING(TDID, 9, 1) = '-'")
      .select('BidRequestId, // use to connect with bidrequest, to get more features
        'AdvertiserId,
        'AdGroupId,
        'SupplyVendor,
        'DealId,
        'SupplyVendorPublisherId,
        'AliasedSupplyPublisherId,
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
        'cos_minute_day,
        'CampaignId,
        'TDID,
        'IDType,
        'DeviceAdvertisingId,
        'CookieTDID,
        'UnifiedId2, 
        'EUID, 
        'IdentityLinkId,
        'LogEntryTime,
        'MatchedSegments
      )
      // they saved in struct type
      .withColumn("SplitRemainder", lit(0))
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
      .withColumn("SyntheticIds", typedLit(Array(0)))
      .withColumn("MatchedSegmentsLength", when('MatchedSegments.isNull,0).otherwise(size('MatchedSegments)).cast(FloatType))
      .withColumn("HasMatchedSegments", when('MatchedSegmentsLength > lit(0), 1).otherwise(0))
      .withColumn("ZipSiteLevel_Seed", typedLit(Array(1)))
      .withColumn("UserSegmentCount", lit(0.0))
      .withColumn("Targets", typedLit(Array(0.0f)))
      .withColumn("SupplyVendor", lit(0))
      .withColumn("AdvertiserId", lit(0))
      .withColumn("SupplyVendorPublisherId", lit(0))
      .withColumn("IdTypesBitmap", idTypesBitmap)
      // sample weight
      .withColumn("SampleWeights", typedLit(Array(1.0f)))
         
      // convert id to long
      .transform(addGuidBits("BidRequestId"))
      .transform(addGuidBits("TDID"))


    val bidsImpressionsTrasnformed = castDoublesToFloat(bidsImpressions)

    val dataset = ModelFeatureTransform.modelFeatureTransform[RelevanceModelInputRecord](bidsImpressionsTrasnformed, readModelFeatures(conf.feature_path))

    val resultSet = paddingColumns(dataset.toDF(),RelevanceModelInputGeneratorConfig.paddingColumns, 0)
                    .cache()

    try {
      RelevanceModelInputDataset(Model.RSMV2.toString,tag = "Imp_Seed_None", version = 2)
      .writePartition(
        dataset = resultSet.as[RelevanceModelInputRecord],
        partition = dateTime,
        format = Some("cbuffer"),
        saveMode = SaveMode.Overwrite,
        numPartitions = Some(10000)
      )
    
    RelevanceModelInputDataset(Model.RSMV2.toString,tag = "Imp_Seed_None", version = 1)
      .writePartition(
        dataset = resultSet.as[RelevanceModelInputRecord],
        partition = dateTime,
        format = Some("tfrecord"),
        saveMode = SaveMode.Overwrite,
        numPartitions = Some(10000)
      )
    } catch {
      case e: Exception =>
        logger.info("error when writing: " + e.getMessage)
        throw e
    }

    logger.info("Finished writing feature dataset.")
  }
}


object Imp2BrModelInferenceDataGenerator
  extends AutoConfigResolvingETLJobBase[Imp2BrModelInferenceDataGeneratorConfig](
    groupName = "audience",
    jobName = "Imp2BrModelInferenceDataGenerator") {

  override val prometheus: Option[PrometheusClient] = Some(new PrometheusClient("audience", "Imp2BrModelInferenceDataGenerator"))

  override def runETLPipeline(): Unit = {
    val conf = getConfig
    new Imp2BrModelInferenceDataGenerator(prometheus.get).run(conf, getLogger)
  }
}


