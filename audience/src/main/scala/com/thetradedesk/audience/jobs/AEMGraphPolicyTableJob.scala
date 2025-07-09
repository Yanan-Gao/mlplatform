package com.thetradedesk.audience.jobs

import com.thetradedesk.audience._
import com.thetradedesk.audience.datasets._
import com.thetradedesk.audience.jobs.policytable.AEMPolicyTableGenerator.{retrieveActiveCampaignConversionTrackerTagIds, samplingFunction}
import com.thetradedesk.spark.TTDSparkContext.spark
import com.thetradedesk.spark.TTDSparkContext.spark.implicits._
import com.thetradedesk.spark.util.TTDConfig.{config, defaultCloudProvider}
import com.thetradedesk.spark.util.prometheus.PrometheusClient
import org.apache.spark.sql._
import org.apache.spark.sql.functions._

import java.sql.Timestamp
import java.time.{LocalDate, LocalDateTime, ZoneOffset}

private val aemPrometheus = new PrometheusClient("AudienceModelJob", "AEMGraphPolicyTableJob")

case class AEMJobConfig(date: LocalDate)

object AEMGraphPolicyTableJob extends AudienceGraphPolicyTableGenerator(
  GoalType.CPA, Model.AEM, aemPrometheus) {

  val prometheus: PrometheusClient = aemPrometheus

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
      .readRange(date.minusDays(Config.conversionLookBack).atStartOfDay(), date.plusDays(1).atStartOfDay())
      .select('TDID, 'TrackingTagId)
      .filter(samplingFunction('TDID))

    conversionDataset.cache()

    val trackingTagDataset = LightTrackingTagDataset().readPartition(date)
      .select("TrackingTagId", "TargetingDataId")
      .distinct()

    val activeConversionTrackerTagId = retrieveActiveCampaignConversionTrackerTagIds()

    if (Config.useSelectedPixel) {
      val selectedTrackingTagIds = spark.read.parquet(Config.selectedPixelsConfigPath)
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
      .repartition(Config.bidImpressionRepartitionNum, 'TDID)
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

  def run(spark: SparkSession, config: AEMJobConfig): Unit = {
    generatePolicyTable()
  }

  def runETLPipeline(): Unit = {
    run(spark, AEMJobConfig(date))
  }

  def main(args: Array[String]): Unit = {
    runETLPipeline()
    prometheus.pushMetrics()
  }
}
