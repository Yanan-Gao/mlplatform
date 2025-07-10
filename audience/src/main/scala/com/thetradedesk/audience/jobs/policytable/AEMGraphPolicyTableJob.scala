package com.thetradedesk.audience.jobs

import com.thetradedesk.audience._
import com.thetradedesk.audience.datasets._
import com.thetradedesk.audience.jobs.policytable.{AggregatedGraphTypeRecord, AudienceGraphPolicyTableGenerator, SourceDataWithDifferentGraphType, SourceMetaRecord, AudiencePolicyTableJobConfig, AudiencePolicyTableGenerator}
import com.thetradedesk.spark.TTDSparkContext.spark
import com.thetradedesk.spark.TTDSparkContext.spark.implicits._
import com.thetradedesk.spark.util.TTDConfig.{config => ttdConfig, defaultCloudProvider}
import com.thetradedesk.spark.util.prometheus.PrometheusClient
import org.apache.spark.sql._
import org.apache.spark.sql.functions._

import java.sql.Timestamp
import java.time.{LocalDate, LocalDateTime, ZoneOffset}

import com.thetradedesk.confetti.AutoConfigResolvingETLJobBase


object AEMGraphPolicyTableJob
  extends AutoConfigResolvingETLJobBase[AudiencePolicyTableJobConfig](
    env = ttdConfig.getStringRequired("env"),
    experimentName = ttdConfig.getStringOption("experimentName"),
    runtimeConfigBasePath = ttdConfig.getStringRequired("confetti_runtime_config_base_path"),
    groupName = "audience",
    jobName = "AEMGraphPolicyTableJob") {

  override val prometheus: Option[PrometheusClient] =
    Some(new PrometheusClient("AudienceModelJob", "AEMGraphPolicyTableJob"))

  private def createGenerator(conf: AudiencePolicyTableJobConfig) =
    new AudienceGraphPolicyTableGenerator(GoalType.CPA, Model.AEM, conf) {
      override def getPrometheus: PrometheusClient = prometheus.get

  private def retrieveActiveCampaignConversionTrackerTagIds(): DataFrame = {
    // prepare dataset
    val Campaign = CampaignDataSet().readLatestPartition()
    val AdGroup = AdGroupDataSet().readLatestPartition()
    val Partner = PartnerDataSet().readLatestPartition()
    val CampConv = CampaignConversionReportingColumnDataset().readLatestPartition()

    // calculate active campaign time range
    val now = LocalDateTime.now(ZoneOffset.UTC)
    val endDateThreshold = now.minusHours(48)
    val startDateThreshold = now.plusHours(48)
    val startTimestamp = Timestamp.valueOf(startDateThreshold)
    val endTimestamp = Timestamp.valueOf(endDateThreshold)

    val activeCampaignConversionTrackerTagIds = AdGroup
      .join(Campaign, AdGroup("CampaignId") === Campaign("CampaignId"))
      .join(Partner, Campaign("PartnerId") === Partner("PartnerId"))
      .join(CampConv, Campaign("CampaignId") === CampConv("CampaignId"))
      .filter(
        AdGroup("IsEnabled") === 1 &&
          Campaign("StartDate").lt(startTimestamp) &&
          (Campaign("EndDate").isNull || Campaign("EndDate").gt(endTimestamp)) &&
          Partner("SpendDisabled") === 0
      )
      .select(CampConv("TrackingTagId")).distinct()

    activeCampaignConversionTrackerTagIds
  }

  override def retrieveSourceMetaData(date: LocalDate): Dataset[SourceMetaRecord] = {
    throw new Exception(s"unsupported in AEMGraphPolicyTableGenerator")
  }

  override def retrieveSourceDataWithDifferentGraphType(date: LocalDate, personGraph: DataFrame, householdGraph: DataFrame): SourceDataWithDifferentGraphType = {
    var conversionDataset = ConversionDataset(defaultCloudProvider)
      .readRange(date.minusDays(config.conversionLookBack).atStartOfDay(), date.plusDays(1).atStartOfDay())
      .select('TDID, 'TrackingTagId)
      .filter(samplingFunction('TDID))

    conversionDataset.cache()

    val trackingTagDataset = LightTrackingTagDataset().readPartition(date)
      .select("TrackingTagId", "TargetingDataId")
      .distinct()

    val activeConversionTrackerTagId = retrieveActiveCampaignConversionTrackerTagIds()

    if (config.useSelectedPixel) {
      val selectedTrackingTagIds = spark.read.parquet(config.selectedPixelsConfigPath)
        .join(trackingTagDataset, "TargetingDataId").select("TrackingTagId")

      conversionDataset =
        conversionDataset.join(selectedTrackingTagIds, "TrackingTagId")
          .join(activeConversionTrackerTagId, "TrackingTagId")

    } else {
      conversionDataset =
        conversionDataset.join(activeConversionTrackerTagId, "TrackingTagId")
    }

    val conversionMeta = conversionDataset
      .groupBy('TrackingTagId)
      .agg(
        countDistinct('TDID)
          .alias("Count"))
      .join(trackingTagDataset, "TrackingTagId")
      .withColumnRenamed("TrackingTagId", "SourceId")
      .select('SourceId, 'Count, 'TargetingDataId, array(lit("")).alias("topCountryByDensity"), lit(DataSource.Conversion.id).alias("Source"), lit(PermissionTag.Private.id).alias("PermissionTag")) // topCountryByDensity is not used for AEM right now
      .as[SourceMetaRecord]

    val TDID2ConversionPixel = conversionDataset
      .repartition(config.bidImpressionRepartitionNum, 'TDID)
      .groupBy('TDID)
      .agg(collect_set('TrackingTagId).alias("SeedIds"))
      .as[AggregatedGraphTypeRecord]

    val personGraphConversionData = readGraphDataAndExtendConversion(conversionDataset, personGraph)(spark)
    val householdGraphConversionData = readGraphDataAndExtendConversion(conversionDataset, householdGraph)(spark)

    val sampledSeedData = TDID2ConversionPixel
      .where(samplingFunction('TDID))

    SourceDataWithDifferentGraphType(
      sampledSeedData,
      personGraphConversionData,
      householdGraphConversionData,
      conversionMeta
    )
  }

  override def getAggregatedSeedWritableDataset(): LightWritableDataset[AggregatedSeedRecord] = AggregatedConversionPixelWritableDataset()

  override def getAggregatedSeedReadableDataset(): LightReadableDataset[AggregatedSeedRecord] = AggregatedConversionPixelReadableDataset()

  private def readGraphDataAndExtendConversion(conversionData: DataFrame, graph: DataFrame)(implicit spark: SparkSession):
  Dataset[AggregatedGraphTypeRecord] = {
    val conversionWithGraph = graph.join(conversionData, Seq("TDID"), "inner")

    val groupIdToConversionIds = conversionWithGraph
      .groupBy('groupId)
      .agg(collect_set('TrackingTagId).alias("SeedIds"))

    val graphGroup = graph
      .groupBy('groupId)
      .agg(collect_set('TDID).alias("TDIDs"))
      .select('groupId, arraySamplingFunction('TDIDs).alias("TDIDs"))
      .where(size('TDIDs) > lit(0))

    val TDID2ConversionIds = groupIdToConversionIds
      .join(graphGroup, Seq("groupId"), "inner")
      .select(explode('TDIDs).alias("TDID"), 'SeedIds)

    TDID2ConversionIds.as[AggregatedGraphTypeRecord]
  }
}

  override def runETLPipeline(): Map[String, String] = {
    val conf = getConfig
    val dt = LocalDateTime.parse(conf.date_time)
    date = dt.toLocalDate
    dateTime = dt
    val generator = createGenerator(conf)
    generator.generatePolicyTable()
    Map("status" -> "success")
  }
}
