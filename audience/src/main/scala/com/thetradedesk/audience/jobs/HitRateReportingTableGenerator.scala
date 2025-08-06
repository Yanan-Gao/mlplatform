package com.thetradedesk.audience.jobs

import com.thetradedesk.audience.configs.AudienceModelInputGeneratorConfig
import com.thetradedesk.audience.datasets.{AggregatedSeedReadableDataset, CampaignFlightDataSet, CampaignSeedDataset, HitRateRecord, HitRateWritableDataset}
import com.thetradedesk.audience.jobs.HitRateReportingTableGeneratorJob.prometheus
import com.thetradedesk.audience.transform.IDTransform.{IDType, filterOnIdTypes, joinOnIdType}
import com.thetradedesk.audience._
import com.thetradedesk.audience.utils.SeedListUtils.activeCampaignSeedAndIdFilterUDF
import com.thetradedesk.geronimo.bidsimpression.schema.{BidsImpressions, BidsImpressionsSchema}
import com.thetradedesk.geronimo.shared.{GERONIMO_DATA_SOURCE, loadParquetData}
import com.thetradedesk.spark.TTDSparkContext.spark
import com.thetradedesk.spark.TTDSparkContext.spark.implicits._
import com.thetradedesk.spark.util.TTDConfig.config
import com.thetradedesk.spark.util.prometheus.PrometheusClient
import org.apache.spark.sql.SaveMode
import org.apache.spark.sql.functions._

object HitRateReportingTableGeneratorJob {
  val prometheus = new PrometheusClient("AudienceMeasurementJob", "HitRateReportingTableGeneratorJob")

  def main(args: Array[String]): Unit = {
    runETLPipeline()
    prometheus.pushMetrics()
  }

  def runETLPipeline(): Unit = {
    HitRateReportingTableGenerator.generateHitRateTable()

  }
}


abstract class HitRateTableGenerator(prometheus: PrometheusClient) {

  val jobRunningTime = prometheus.createGauge(s"audience_hitrate_table_job_running_time", "HitRateReportingTableGenerator running time", "date")
  val hitRateTableSize = prometheus.createGauge(s"audience_hitrate_table_size", "HitRateReportingTableGenerator table size", "date")
  val sampleUDF = shouldConsiderTDID3(config.getInt("hitRateUserDownSampleHitPopulation", default = 1000000), config.getString("saltToSampleHitRate", default = "0BgGCE"))(_)


  def generateHitRateTable(): Unit = {

    val start = System.currentTimeMillis()

    val (activeCampaignSeed, activeSeedIdFilterUDF) = activeCampaignSeedAndIdFilterUDF()

    val seedData = AggregatedSeedReadableDataset()
      .readPartition(date)(spark)
      .select('TDID
        , 'idType
        , activeSeedIdFilterUDF('SeedIds).as("SeedIds")
        , activeSeedIdFilterUDF('PersonGraphSeedIds).as("PersonGraphSeedIds")
        , activeSeedIdFilterUDF('HouseholdGraphSeedIds).as("HouseholdGraphSeedIds"))
      .repartition(AudienceModelInputGeneratorConfig.bidImpressionRepartitionNumAfterFilter, 'TDID)
      .cache()

    val bidImpressionsS3Path = BidsImpressions.BIDSIMPRESSIONSS3 + "prod/bidsimpressions/"
    val bidsImpressions = loadParquetData[BidsImpressionsSchema](bidImpressionsS3Path, date, lookBack = Some(0), source = Some(GERONIMO_DATA_SOURCE))
      .where(filterOnIdTypes(sampleUDF))
      .withColumn("ReportDate", to_date('LogEntryTime))
      .select('BidRequestId, // use to connect with bidrequest, to get more features
        'AdvertiserId,
        'CampaignId,
        'AdGroupId,
        'ReportDate,
        'CookieTDID,
        'DeviceAdvertisingId,
        'UnifiedId2,
        'EUID,
        'IdentityLinkId,
        'DATId
      )
      .filter('IsImp)
      .cache()

    val result = joinOnIdType(bidsImpressions, seedData.toDF(), IDType.CookieTDID ,"left")
      .union(IDType.values
        .filter(e => e != IDType.Unknown && e != IDType.CookieTDID)
        .map(e => joinOnIdType(bidsImpressions, seedData.toDF(), e))
        .reduce(_ union _))
      .withColumn("SeedIds", coalesce('SeedIds, array()))
      .withColumn("PersonGraphSeedIds", coalesce('PersonGraphSeedIds, array()))
      .withColumn("HouseholdGraphSeedIds", coalesce('HouseholdGraphSeedIds, array()))
      .groupBy('BidRequestId,
        'AdvertiserId,
        'CampaignId,
        'AdGroupId,
        'ReportDate)
      .agg(flatten(collect_list('SeedIds)).as("SeedIds"),
        flatten(collect_list('PersonGraphSeedIds)).as("PersonGraphSeedIds"),
          flatten(collect_list('HouseholdGraphSeedIds)).as("HouseholdGraphSeedIds"))
      .join(broadcast(activeCampaignSeed.select('CampaignId, 'SeedId)), Seq("CampaignId"), "left")
      .withColumn("hit", array_contains($"SeedIds", $"SeedId"))
      .withColumn("personGraphHit", ('hit || array_contains($"PersonGraphSeedIds", $"SeedId")).cast("integer"))
      .withColumn("HHGraphHit", ('hit || array_contains($"HouseholdGraphSeedIds", $"SeedId")).cast("integer"))
      .withColumn("hit", 'hit.cast("integer"))

    val hitRate = result.groupBy('CampaignId, 'AdGroupId, 'SeedId, 'ReportDate)
      .agg(
        count('hit).alias("ImpressionCount")
        , sum(when(size('SeedIds) > 0, 1).otherwise(0)).alias("SeedImpressionCount")
        , sum(when(size('SeedIds) > 0 || size('PersonGraphSeedIds) > 0, 1).otherwise(0)).alias("PersonGraphSeedImpressionCount")
        , sum(when(size('SeedIds) > 0 || size('HouseholdGraphSeedIds) > 0, 1).otherwise(0)).alias("HHGraphSeedImpressionCount")
        , sum('hit).alias("HitCount")
        , sum('personGraphHit).alias("PersonGraphHitCount")
        , sum('HHGraphHit).alias("HHGraphHitCount")
      )
      .withColumn("HitRate", 'HitCount / 'ImpressionCount)
      .withColumn("PersonGraphHitRate", 'PersonGraphHitCount / 'ImpressionCount)
      .withColumn("HHGraphHitRate", 'HHGraphHitCount / 'ImpressionCount)
      .as[HitRateRecord]

    HitRateWritableDataset()
      .writePartition(hitRate,
        date,
        saveMode = SaveMode.Overwrite)

    hitRateTableSize.labels(dateTime.toLocalDate.toString).set(hitRate.count())
    jobRunningTime.labels(dateTime.toLocalDate.toString).set(System.currentTimeMillis() - start)
  }

}

object HitRateReportingTableGenerator extends HitRateTableGenerator(prometheus: PrometheusClient) {
}