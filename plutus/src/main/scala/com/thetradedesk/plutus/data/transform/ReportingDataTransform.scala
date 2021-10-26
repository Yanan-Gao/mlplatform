package com.thetradedesk.plutus.data.transform

import com.thetradedesk.plutus.data.loadParquetData
import com.thetradedesk.plutus.data.schema.AdGroupDataset.ADGROUPS3
import com.thetradedesk.plutus.data.schema.{AdGroupDataset, AdGroupRecord, BidFeedbackDataset, BidRequestDataset, BidRequestRecordV4, Impressions, ReportingData, VerticaKoaVolumeControlBudgetRecord}
import com.thetradedesk.spark.TTDSparkContext.spark
import com.thetradedesk.spark.TTDSparkContext.spark.implicits._
import com.thetradedesk.spark.util.prometheus.PrometheusClient
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{Column, DataFrame}
import org.apache.spark.sql.types.DoubleType

import java.time.LocalDate

object ReportingDataTransform {

  val ColumnList: List[Column] = List($"bidrequestId",
    $"adgroupId",
    $"testId",
    $"BidderCacheMachineName",
    $"logEntryTime",
    $"model")

  def transform(date: LocalDate, model: String, testIdStringMatch: String, dataCenters: Seq[String])(implicit prometheus: PrometheusClient): Unit ={

    val avgCpm = prometheus.createGauge("average_cpm", "average cpm per model", labelNames = "model")
    val savingsPerImp = prometheus.createGauge("savings_per_imp", "savings per impression per model", labelNames = "model")
    val pctStarvingAg = prometheus.createGauge("pct_starving_adgoups", "percentage of adgroups starving per type", labelNames = "model")
    val winRate = prometheus.createGauge("win_rate", "win rate per model", labelNames = "model")
    val totalRows = prometheus.createGauge("plutus_reporting_rows", "total rows of bid used in plutus reporting")

    val allBids = getBiddingData(date, model, testIdStringMatch, dataCenters)
    val bidFeedback = loadParquetData[Impressions](s3path = BidFeedbackDataset.BFS3, date = date)
      .select($"bidrequestId",
        ($"SubmittedBidAmountInUSD" * (lit(1.0d) - $"FirstPriceAdjustment")).alias("savings"),
        $"MediaCostCPMInUSD",
        $"bidfeedbackId"
      )

    // val agData = allBids.select($"adgroupId" , $"model").distinct

    //val starvingAGs = starvingAdGroups(agData, date).cache()

    val bfBr = allBids.join(bidFeedback, Seq("bidRequestId") , "left")
      .withColumn("wins", when($"bidfeedbackId".isNull, 0).otherwise(1))

    val output = bfBr.groupBy($"model")
      .agg(count($"*").alias("totalCount"),
        sum($"wins").alias("totalWins"),
        sum($"savings").alias("totalSavings"),
        avg($"mediaCostCPMinUSD").cast(DoubleType).alias("avgMediaCostCpm"))
      .withColumn("savingsPerImp" , ($"totalSavings" / $"totalWins").cast(DoubleType))
      .withColumn("winRate", ($"totalWins" / $"totalCount").cast(DoubleType))
      .cache()

    totalRows.set(output.count())
    
    // println("total ags with starving data: " + starvingAGs.count())

    val models = output.select($"model").collect.map(_.getString(0))

    for (model <- models)
    {
      val modelDat = output.filter($"model" === model)
      // val agDat = starvingAGs.filter($"model" === model)
      avgCpm.labels(model).set(modelDat.select($"avgMediaCostCpm").head.getDouble(0))
      savingsPerImp.labels(model).set(modelDat.select($"savingsPerImp").head.getDouble(0))
      winRate.labels(model).set(modelDat.select($"winRate").head.getDouble(0))
      // pctStarvingAg.labels(model).set(agDat.select($"starvationRatio").head.getDouble(0))

    }



  }

  def getBiddingData(date: LocalDate, model: String, testIdStringMatch: String, dataCenters: Seq[String]) = {
    val baseBiddingData = loadParquetData[BidRequestRecordV4](BidRequestDataset.BIDSS3, date)
      .select($"bidrequestId",
        $"adgroupId",
        $"testId",
        $"BidderCacheMachineName",
        $"logEntryTime"
      )

    val adgroupPcStatus = loadParquetData[AdGroupRecord](ADGROUPS3, date).select($"adgroupId", $"PredictiveClearingEnabled")

    val plutusBidding = baseBiddingData
      .withColumn("model", lit(model))
      .filter($"testId".like(testIdStringMatch + "%")  && ($"BidderCacheMachineName".like("de1%") || $"BidderCacheMachineName".like("ca2%")))

    val nonPlutusAgData = baseBiddingData
      .filter(!$"testId".like(testIdStringMatch + "%") && ($"BidderCacheMachineName".like("de1%") || $"BidderCacheMachineName".like("ca2%")))
      .join(adgroupPcStatus, Seq("adgroupId"), "inner")

    val legacyPc = nonPlutusAgData
      .filter($"PredictiveClearingEnabled")
      .withColumn("model" , lit("LegacyPc"))
      .select(ColumnList: _*)

    val baseLine = nonPlutusAgData
      .filter(!$"predictiveClearingEnabled")
      .withColumn("model" , lit("None"))
      .select(ColumnList: _*)

    plutusBidding.union(legacyPc).union(baseLine)

  }

  def starvingAdGroups(adGroupData: DataFrame, date: LocalDate ) = {
    val budgetData = loadParquetData[VerticaKoaVolumeControlBudgetRecord](ReportingData.verticaKoaVolumeControlBudgetS3, date)
      .select($"adgroupId", $"PotentialSpendRatio")
      .withColumn("MaxRatio", max($"PotentialSpendRatio").over(Window.partitionBy($"AdGroupId")))
      .filter($"MaxRatio" < 1.05)
      .select($"AdGroupId").distinct
      .withColumn("starveLevel" , lit(true))

    adGroupData.join(budgetData, Seq("adgroupId"), "left")
      .withColumn("isStarving" , when($"starveLevel".isNull, 0).otherwise(1))
      .groupBy($"model")
      .agg(count("*").alias("uniqueAdgroups"),
        sum($"isStarving").alias("starvingCount"))
      .withColumn("starvationRatio" , ($"starvingCount" / $"uniqueAdgroups").cast(DoubleType))
  }

}
