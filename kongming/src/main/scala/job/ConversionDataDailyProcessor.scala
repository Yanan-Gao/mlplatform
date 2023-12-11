package job

import com.thetradedesk.kongming.datasets._
import com.thetradedesk.spark.TTDSparkContext.spark.implicits._
import com.thetradedesk.spark.util.TTDConfig.{config, defaultCloudProvider}
import com.thetradedesk.kongming._
import com.thetradedesk.kongming.transform.ConversionDataDailyTransform
import com.thetradedesk.spark.datasets.sources.datalake.ConversionTrackerVerticaLoadDataSetV4
import com.thetradedesk.spark.sql.SQLFunctions._
import org.apache.spark.sql.{DataFrame, Dataset}
import org.apache.spark.sql.functions._

import java.time.LocalDate


object ConversionDataDailyProcessor extends KongmingBaseJob {
  val graphThreshold: Double = config.getDouble("graphThreshold", default = 0.01)//TODO: verify what's a good value here.
  val bidLookback = config.getInt("bidLookback", default = 20)

  // TODO: set true for roas
  val redate = config.getBoolean("redate", default = false)

  override def jobName: String = "DailyConversion"

  /**
   * @param lookbackDate date of historical data partition to union
   * @param todayConversions today's conversions
   * @return if historical data can be found, returns dailyConversions union with historical conversions of specified date;
   *         else returns todayConversions filtered conversions of specified date.
   */
  private def getConversionWithHistoricalData(lookbackDate: LocalDate, todayConversions: DataFrame): Dataset[DailyConversionDataRecord] = {
    val dailyConversions = todayConversions.filter($"ConversionDate" === lit(lookbackDate)).selectAs[DailyConversionDataRecord]

    if (DailyConversionDataset().partitionExists(lookbackDate)) {
      val histConversions = DailyConversionDataset().readDate(lookbackDate).selectAs[DailyConversionDataRecord]
      dailyConversions.union(histConversions).distinct
    } else {
      dailyConversions
    }
  }

  override def runTransform(args: Array[String]): Array[(String, Long)] = {

    // read conversion data daily
    val conversionDS = ConversionTrackerVerticaLoadDataSetV4(defaultCloudProvider).readDate(date)
    // read campaign conversion reporting column setting
    val ccrc = CampaignConversionReportingColumnDataSet().readLatestPartitionUpTo(date, true)
    // read master policy
    val adGroupPolicy = AdGroupPolicyDataset().readDate(date)
    // read adgroup table to get adgroup campaign mapping
    val adGroupMapping = AdGroupPolicyMappingDataset().readDate(date)
    // read campaign table to get setting
    val campaignDS = CampaignDataSet().readLatestPartitionUpTo(date, true)

    //filter down conversion data and add weights from conversion reporting column table
    //transformedConvDS is returned as cached datasets.
    val (transformedConvDS, idDS) = ConversionDataDailyTransform.dailyTransform(
      conversionDS,
      ccrc,
      adGroupPolicy,
      adGroupMapping,
      campaignDS
    )(getPrometheus)

    //load cross device graph
    val xdDS = CrossDeviceGraphDataset.loadGraph(date, graphThreshold)//TODO: this threshold will need to be based on policy table minimum.
    val xdSubsetDS = CrossDeviceGraphDataset.shrinkGraph(xdDS, idDS) // returned result is cached

    //add xd according to adgroupPolicy
    val transformedConvXD = transformedConvDS.filter($"CrossDeviceAttributionModelId".isNotNull)
    val conversionNonXD = transformedConvDS
      //removing this following filter to include everything without cross device
      //.filter(!$"CrossDeviceUsage")
      .withColumnRenamed("TDID", "UIID")
      .selectAs[DailyConversionDataRecord]
      .distinct

    val conversionXD = ConversionDataDailyTransform.addCrossDeviceTransform(
      transformedConvXD
      , xdSubsetDS
    )(getPrometheus)

    //add distinct to remove rare cases when there's two ids under the same person converted under the same trackingtag.
    val unionConversion = conversionNonXD.union(conversionXD)
    val resultDS = (
      task match {
        case "roas" => unionConversion.distinct()
        // offline conversions could have multiple monetary values under the same ("TrackingTagId","UIID","DataAggKey","DataAggValue","ConversionTime")
        case _ => unionConversion.dropDuplicates("TrackingTagId", "UIID", "ConfigKey", "ConfigValue", "ConversionTime")
      }
    )

    var rowCounts = if (redate) new Array[(String, Long)](bidLookback+1) else new Array[(String, Long)](1)

    if (redate) {
      // to repartition today's conversion based on their actual conversion date
      val todayConversions = resultDS.withColumn("ConversionDate", to_date($"ConversionTime"))
        .cache

      (0 to bidLookback).par.foreach(i => {
        // read, union, and write in parallel
        val lookbackDate = date.minusDays(bidLookback - i)
        val conversions = getConversionWithHistoricalData(lookbackDate, todayConversions)

        val dailyConversionRows = DailyConversionDataset().writePartition(conversions, lookbackDate, Some(partCount.DailyConversion))
        rowCounts(i) = dailyConversionRows
      })
    } else {
      val dailyConversionRows = DailyConversionDataset().writePartition(resultDS, date, Some(partCount.DailyConversion))
      rowCounts(0) = dailyConversionRows
    }

    rowCounts
  }
}
