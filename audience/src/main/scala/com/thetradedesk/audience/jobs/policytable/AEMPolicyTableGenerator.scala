package com.thetradedesk.audience.jobs.policytable

import com.thetradedesk.audience._
import com.thetradedesk.audience.datasets._
import com.thetradedesk.audience.jobs.policytable.AudiencePolicyTableGeneratorJob.prometheus
import com.thetradedesk.spark.TTDSparkContext.spark
import com.thetradedesk.spark.TTDSparkContext.spark.implicits._
import com.thetradedesk.spark.util.TTDConfig.{config, defaultCloudProvider}
import com.thetradedesk.spark.util.prometheus.PrometheusClient
import org.apache.spark.sql._
import org.apache.spark.sql.functions._

import java.sql.Timestamp
import java.time.{LocalDate, LocalDateTime, ZoneOffset}

object AEMPolicyTableGenerator extends AudiencePolicyTableGenerator(Model.AEM, prometheus: PrometheusClient) {

  override def retrieveSourceData(date: LocalDate): DataFrame = {
    retrieveConversionData(date: LocalDate)
  }

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

  private def retrieveConversionData(date: LocalDate): DataFrame = {
    val uniqueTDIDsFromBidImp = getBidImpUniqueTDIDs(date).select("TDID")

    // conversion
    val activeConversionTrackerTagId = retrieveActiveCampaignConversionTrackerTagIds();

    var conversionDataset = ConversionDataset(defaultCloudProvider)
      .readRange(date.minusDays(Config.conversionLookBack).atStartOfDay(), date.plusDays(1).atStartOfDay())
      .select('TDID, 'TrackingTagId)
      .filter(samplingFunction('TDID))

    conversionDataset.cache();

    val trackingTagDataset = LightTrackingTagDataset().readPartition(date)
      .select("TrackingTagId", "TargetingDataId")
      .distinct()

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

    val conversionSize = conversionDataset
      .groupBy('TrackingTagId)
      .agg(
        countDistinct('TDID)
          .alias("Size"))

    val conversionActiveSize = conversionDataset.join(uniqueTDIDsFromBidImp, "TDID")
      .groupBy('TrackingTagId)
      .agg(
        countDistinct('TDID)
          .alias("ActiveSize"))
      .select(('ActiveSize * (userDownSampleBasePopulation / userDownSampleHitPopulation)).alias("ActiveSize"), 'TrackingTagId)

    var conversionFinal: DataFrame = null

    if (Config.useSelectedPixel) {
      conversionFinal = conversionSize
        .join(trackingTagDataset, "TrackingTagId")
        .join(conversionActiveSize, "TrackingTagId")
    } else {
      conversionFinal = conversionSize
        .join(conversionActiveSize, "TrackingTagId")
        .join(trackingTagDataset, "TrackingTagId")
        .orderBy(desc("ActiveSize"))
        .limit(Config.aemPixelLimit)
    }

    val policyTable = conversionFinal
      .withColumn("Source", lit(DataSource.Conversion.id))
      .withColumn("GoalType", lit(GoalType.CPA.id))
      .withColumn("CrossDeviceVendorId", lit(CrossDeviceVendor.None.id))
      .withColumnRenamed("TrackingTagId", "SourceId")


    policyTable
  }
}
