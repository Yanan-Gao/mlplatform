package com.thetradedesk.audience.jobs

import com.thetradedesk.audience.configs.AudienceModelInputGeneratorConfig
import com.thetradedesk.audience.datasets.{AggregatedSeedReadableDataset, CampaignSeedDataset, HitRateReadableDataset, HitRateRecord, HitRateWritableDataset}
import com.thetradedesk.audience.utils.Logger.Log
import com.thetradedesk.audience.{shouldConsiderTDID3, _}
import com.thetradedesk.audience.jobs.HitRateReportingTableGeneratorJob.prometheus
import com.thetradedesk.geronimo.bidsimpression.schema.{BidsImpressions, BidsImpressionsSchema}
import com.thetradedesk.geronimo.shared.{GERONIMO_DATA_SOURCE, loadParquetData}
import com.thetradedesk.spark.TTDSparkContext.spark
import com.thetradedesk.spark.TTDSparkContext.spark.implicits._
import com.thetradedesk.spark.util.TTDConfig.{config, defaultCloudProvider}
import com.thetradedesk.spark.util.io.FSUtils
import com.thetradedesk.spark.util.prometheus.PrometheusClient
import org.apache.commons.lang3.exception.ExceptionUtils
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.{ArrayType, StringType, StructType}
import org.apache.spark.sql.{DataFrame, Dataset, Row, SaveMode, SparkSession}

import java.time.format.DateTimeFormatter
import java.time.temporal.ChronoUnit
import java.time.{LocalDate, LocalDateTime}
import scala.util.Random

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

    val isStringInArray = udf((str: String, arr: Seq[String]) => {
      Option(arr).exists(_.contains(str))
    })

    val campaignSeed = CampaignSeedDataset().readPartition(dateTime.toLocalDate)

    val seedData = AggregatedSeedReadableDataset()
      .readPartition(date)(spark)
      .repartition(AudienceModelInputGeneratorConfig.bidImpressionRepartitionNumAfterFilter, 'TDID)
      .cache()

    val bidImpressionsS3Path = BidsImpressions.BIDSIMPRESSIONSS3 + "prod/bidsimpressions/"
    val bidsImpressions = loadParquetData[BidsImpressionsSchema](bidImpressionsS3Path, date, lookBack = Some(0), source = Some(GERONIMO_DATA_SOURCE))
      .withColumnRenamed("UIID", "TDID")
      .where(sampleUDF('TDID))
      .withColumn("ReportDate", to_date('LogEntryTime))
      .select('BidRequestId, // use to connect with bidrequest, to get more features
        'AdvertiserId,
        'CampaignId,
        'AdGroupId,
        'ReportDate,
        'TDID
      )
      .filter('IsImp)
      .repartition(AudienceModelInputGeneratorConfig.bidImpressionRepartitionNumAfterFilter, 'TDID)
      .cache()

    val result = bidsImpressions
      .join(campaignSeed.select('CampaignId, 'SeedId), Seq("CampaignId"), "left")
      .join(seedData.filter(size('SeedIds) > 0).select('TDID, 'SeedIds), Seq("TDID"), "left")
      .withColumn("hit", isStringInArray($"SeedId", $"SeedIds").cast("integer"))

    val hitRate = result.groupBy('CampaignId, 'AdGroupId, 'SeedId, 'ReportDate)
      .agg(
        count('hit).alias("ImpressionCount")
        , sum(when(size('SeedIds) > 0, 1).otherwise(0)).alias("SeedImpressionCount")
        , sum('hit).alias("HitCount")
      )
      .withColumn("HitRate", 'HitCount / 'ImpressionCount)
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