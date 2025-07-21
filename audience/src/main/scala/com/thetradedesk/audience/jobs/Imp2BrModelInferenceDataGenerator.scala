package com.thetradedesk.audience.jobs
import com.thetradedesk.audience.datasets.{AudienceModelInputDataset, AudienceModelInputRecord, Model}
// Avoid importing the audience package object here because it eagerly reads
// configuration when loaded, which fails on Spark executors during
// deserialization. Only the "DoNotTrackTDID" constant is required.
import com.thetradedesk.audience.jobs.modelinput.rsmv2.usersampling.SIBSampler._isDeviceIdSampled1Percent
import com.thetradedesk.geronimo.bidsimpression.schema.{BidsImpressions, BidsImpressionsSchema}
import com.thetradedesk.geronimo.shared.transform.ModelFeatureTransform
import com.thetradedesk.geronimo.shared.{GERONIMO_DATA_SOURCE, loadParquetData, readModelFeatures}
import org.apache.spark.sql.functions._
import com.thetradedesk.spark.TTDSparkContext.spark.implicits._
import org.apache.spark.sql.SaveMode
import org.apache.spark.sql.types.FloatType
import com.thetradedesk.confetti.AutoConfigResolvingETLJobBase
import com.thetradedesk.spark.util.prometheus.PrometheusClient

import scala.collection.mutable.ArrayBuffer
import java.time.LocalDate

case class Imp2BrModelInferenceDataGeneratorConfig(
  feature_path: String,
  runDate: LocalDate
)

object Imp2BrModelInferenceDataGenerator
  extends AutoConfigResolvingETLJobBase[Imp2BrModelInferenceDataGeneratorConfig](
    groupName = "audience",
    jobName = "Imp2BrModelInferenceDataGenerator") {

  override val prometheus: Option[PrometheusClient] = None

  // Constant representing the anonymized TDID value. Declared here to avoid
  // forcing initialization of the `audience` package object on executors.
  private val DoNotTrackTDID = "00000000-0000-0000-0000-000000000000"

  /***
   * Generate RMSv2 BidRequest model offline prediction input dataset, based on BidImpression,
   * with sampling logic aligned with SIBv3
   */
  val bidImpressionsS3Path = BidsImpressions.BIDSIMPRESSIONSS3 + "prod/bidsimpressions/"

  def getAllUiidsUdfWithSample(sampleFun: String => Boolean) = udf((tdid: String, deviceAdvertisingId: String, uid2: String, euid: String, identityLinkId: String) => {
    val uiids = ArrayBuffer[String]()

    // when CookieTDID == DeviceAdvertisingId, keep the latter
    // we don't expect clash among other id types
    if (tdid != null && tdid != DoNotTrackTDID && sampleFun(tdid) && tdid != deviceAdvertisingId) {
      uiids += tdid
    }
    if (deviceAdvertisingId != null && deviceAdvertisingId != DoNotTrackTDID && sampleFun(deviceAdvertisingId)) {
      uiids += deviceAdvertisingId
    }
    if (uid2 != null && uid2 != DoNotTrackTDID && sampleFun(uid2)) {
      uiids += uid2
    }
    if (euid != null && euid != DoNotTrackTDID && sampleFun(euid)) {
      uiids += euid
    }
    if (identityLinkId != null && identityLinkId != DoNotTrackTDID && sampleFun(identityLinkId)) {
      uiids += identityLinkId
    }
    uiids.toSeq
  })

  override def runETLPipeline(): Unit = {
    val conf = getConfig
    val date = conf.runDate
    val dateTime = conf.runDate.atStartOfDay()

    val featuresJsonPath = conf.feature_path

    val bidsImpressions = loadParquetData[BidsImpressionsSchema](bidImpressionsS3Path, date, lookBack=Some(0), source = Some(GERONIMO_DATA_SOURCE))
      .withColumn("Uiids", getAllUiidsUdfWithSample(_isDeviceIdSampled1Percent)('CookieTDID, 'DeviceAdvertisingId, 'UnifiedId2, 'EUID, 'IdentityLinkId))
      .drop("Uiids")
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
        'LogEntryTime,
        'ContextualCategories,
        'MatchedSegments
      )
      // they saved in struct type
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
      .withColumn("SyntheticIds", typedLit(Seq.empty[Int]))
      .withColumn("MatchedSegmentsLength", when('MatchedSegments.isNull,0).otherwise(size('MatchedSegments)).cast(FloatType))
      .withColumn("HasMatchedSegments", when('MatchedSegmentsLength > lit(0), 1).otherwise(0))
      .withColumn("ZipSiteLevel_Seed", typedLit(Seq.empty[Int]))
      .withColumn("GroupId", 'TDID) // TODO:
      .withColumn("ContextualCategoriesTier1", typedLit(Array.empty[Int]))
      .withColumn("UserSegmentCount", lit(0.0))
      .withColumn("Targets", typedLit(Array.empty[Float]))
      .withColumn("SupplyVendor", lit(0))
      .withColumn("AdvertiserId", lit(0))
      .withColumn("SupplyVendorPublisherId", lit(0))

    val dataset = ModelFeatureTransform.modelFeatureTransform[AudienceModelInputRecord](bidsImpressions, readModelFeatures(featuresJsonPath))
    AudienceModelInputDataset(Model.RSMV2.toString,tag = "Imp_Seed_None", version = 1)
      .writePartition(
        dataset = dataset.as[AudienceModelInputRecord],
        partition = dateTime,
        format = Some("tfrecord"),
        saveMode = SaveMode.Overwrite,
        numPartitions = Some(10000)
      )
  }
}
