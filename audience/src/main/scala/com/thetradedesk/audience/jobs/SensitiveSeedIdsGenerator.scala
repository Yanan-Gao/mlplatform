package com.thetradedesk.audience.jobs

import com.thetradedesk.audience.datasets._
import com.thetradedesk.audience.{date, dateTime}
import com.thetradedesk.audience.jobs.modelinput.rsmv2.usersampling.SIBSampler.isDeviceIdSampled
import com.thetradedesk.geronimo.bidsimpression.schema.{BidsImpressions, BidsImpressionsSchema}
import com.thetradedesk.geronimo.shared.{GERONIMO_DATA_SOURCE, loadParquetData}
import com.thetradedesk.spark.TTDSparkContext.spark
import com.thetradedesk.spark.TTDSparkContext.spark.implicits._
import com.thetradedesk.spark.sql.SQLFunctions.{DataSetExtensions, SymbolExtensions}
import com.thetradedesk.spark.util.TTDConfig.config
import com.thetradedesk.spark.util.prometheus.PrometheusClient
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.apache.spark.sql.{Column, DataFrame, SaveMode}

import java.time.LocalDateTime
import java.time.format.DateTimeFormatter

object SensitiveSeedIdsGenerator {
  val DensityScoreS3Path = "s3://thetradedesk-mlplatform-us-east-1/users/yixuan.zheng/trm_play/albertson/1105/user_zip_site/"
  val prometheus = new PrometheusClient("DiagnosisJob", "DiagnosisDataMonitor")
  val diagnosisMetricsCount = prometheus.createGauge("distributed_algo_diagnosis_metrics_count", "Hourly counts of metrics from distributed algo diagnosis pipeline", "roigoaltype", "type")
  val campaignFlightStartingBufferInDays = config.getInt("campaignFlightStartingBufferInDays", 14)



  def runETLPipeline(): Unit = {
    val activeSeeds = AudienceModelPolicyReadableDataset(Model.RSM).readSinglePartition(LocalDateTime.of(date.getYear, date.getMonthValue, date.getDayOfMonth, 0, 0, 0))
      .filter('IsActive==="true" &&  'CrossDeviceVendorId===0)
      .select('SourceId.as("SeedId"), 'SyntheticId, 'IsSensitive)
      .distinct()

    // Get active seed ids
    val startDateTimeStr = DateTimeFormatter.ofPattern("yyyy-MM-dd 00:00:00").format(date.plusDays(campaignFlightStartingBufferInDays))
    val endDateTimeStr = DateTimeFormatter.ofPattern("yyyy-MM-dd 00:00:00").format(date)

    val campaignFlight = CampaignFlightDataSet()
      .readPartition(date)
      .where('IsDeleted === false
        && 'StartDateInclusiveUTC.leq(startDateTimeStr)
        && ('EndDateExclusiveUTC.isNull || 'EndDateExclusiveUTC.gt(endDateTimeStr)))
      .select('CampaignId)
      .distinct()

    val campaignSeed = CampaignSeedDataset()
      .readPartition(date)

    val optInSeed = campaignSeed
      .join(campaignFlight, Seq("CampaignId"), "inner")
      .select('SeedId)
      .distinct()

    val dfSeed: DataFrame = activeSeeds.join(optInSeed, Seq("SeedId"))
      .select('SeedId, 'SyntheticId, 'IsSensitive)
      .distinct()
      .cache()

    val sensitive = dfSeed.filter('IsSensitive === lit(true)).collect()
    val sensitiveSeedIds = sensitive.map(row => row.get(0))
    val sensitiveSyntheticIds = sensitive.map(row => row.get(1))

    val insensitive = dfSeed.filter('IsSensitive === lit(false)).collect()
    val insensitiveSeedIds = insensitive.map(row => row.get(0))
    val insensitiveSyntheticIds = insensitive.map(row => row.get(1))

    val seedIdsSenAndInSen = sensitiveSeedIds ++ insensitiveSeedIds
    val SyntheticIdsSenAndInSen = sensitiveSyntheticIds ++ insensitiveSyntheticIds

    val colAllSeedIds = array(seedIdsSenAndInSen.map(x => lit(x)):_*)
    val colAllSyntheticIds = array(SyntheticIdsSenAndInSen.map(x => lit(x)):_*)

    val colSensitiveSeedsIds = array(sensitiveSeedIds.map(x => lit(x)):_*)
    val colSensitiveSyntheticIds = array(sensitiveSyntheticIds.map(x => lit(x)):_*)

    val colInsensitiveSeedsIds = array(insensitiveSeedIds.map(x => lit(x)):_*)
    val colInsensitiveSyntheticIds = array(insensitiveSyntheticIds.map(x => lit(x)):_*)

    val values = List(List("1", "One")).map(x =>(x(0), x(1)))
    val dfSeedOneRow = values.toDF
      .withColumn("SeedIds", colAllSeedIds)
      .withColumn("SyntheticIds", colAllSyntheticIds)
      .withColumn("SensitiveSeedIds", colSensitiveSeedsIds)
      .withColumn("SensitiveSyntheticIds", colSensitiveSyntheticIds)
      .withColumn("InsensitiveSeedIds", colInsensitiveSeedsIds)
      .withColumn("InsensitiveSyntheticIds", colInsensitiveSyntheticIds)
      .select("SeedIds", "SyntheticIds", "SensitiveSeedIds", "SensitiveSyntheticIds", "InsensitiveSeedIds", "InsensitiveSyntheticIds")
      .selectAs[RelevanceSeedsRecord]

    RelevanceSeedsDataset().writePartition(
      dfSeedOneRow,
      date,
      saveMode = SaveMode.Overwrite,
      format = Some("parquet"))
  }

  def main(args: Array[String]): Unit = {
    runETLPipeline()
    //prometheus.pushMetrics()
  }
}