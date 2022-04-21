package job

import com.thetradedesk.geronimo.shared.loadParquetData
import com.thetradedesk.kongming.datasets.CampaignDataset
import com.thetradedesk.kongming.datasets.CampaignRecord
import com.thetradedesk.kongming.datasets.DailyConversionDataRecord
import com.thetradedesk.kongming.datasets.DailyConversionDataset
import com.thetradedesk.logging.Logger
import com.thetradedesk.spark.TTDSparkContext.spark.implicits._
import com.thetradedesk.spark.util.TTDConfig.config
import com.thetradedesk.kongming.datasets.{AdGroupDataset, AdGroupPolicyDataset, AdGroupRecord, CampaignConversionReportingColumnDataSet, CampaignConversionReportingColumnRecord, ConversionDataset, ConversionRecord, CrossDeviceGraphDataset}
import com.thetradedesk.kongming.transform.ConversionDataDailyTransform
import com.thetradedesk.spark.util.prometheus.PrometheusClient
import com.thetradedesk.spark.TTDSparkContext.spark
import com.thetradedesk.kongming.date

import java.time.LocalDate


object ConversionDataDailyProcessor extends Logger{
  val graphThreshold = config.getDouble("graphThreshold", default = 0.8)//TODO: verify what's a good value here.

  def main(args: Array[String]): Unit = {
    val prometheus = new PrometheusClient("KoaV4Conversion", "DailyConversion")
    // read conversion data daily
    val conversionDS = loadParquetData[ConversionRecord](ConversionDataset.S3Path, date)

    // read campaign conversion reporting column setting
    val ccrc = loadParquetData[CampaignConversionReportingColumnRecord](CampaignConversionReportingColumnDataSet.S3Path, date)

    // read master policy
    val adGroupPolicyHardCodedDate = LocalDate.parse("2022-03-15")
    val adGroupPolicy = AdGroupPolicyDataset.readHardCodedDataset(adGroupPolicyHardCodedDate)

    // read adgroup table to get adgroup campaign mapping
    val adGroupDS = loadParquetData[AdGroupRecord](AdGroupDataset.ADGROUPS3, date)
    // read campaign table to get setting
    val campaignDS = loadParquetData[CampaignRecord](CampaignDataset.S3Path, date)

    //filter down conversion data and add weights from conversion reporting column table
    //transformedConvDS is returned as cached datasets.
    val (transformedConvDS, idDS) = ConversionDataDailyTransform.dailyTransform(
      conversionDS,
      ccrc,
      adGroupPolicy,
      adGroupDS,
      campaignDS
    )(prometheus)

    //load cross device graph
    val xdDS = CrossDeviceGraphDataset.loadGraph(date, graphThreshold)//TODO: this threshold will need to be based on policy table minimum.
    val xdSubsetDS = CrossDeviceGraphDataset.shrinkGraph(xdDS, idDS) // returned result is cached


    //add xd according to adgroupPolicy
    val transformedConvXD = transformedConvDS.filter($"CrossDeviceUsage")
    val conversionNonXD = transformedConvDS
      //removing this following filter to include everything without cross device
      //.filter(!$"CrossDeviceUsage")
      .select(
        $"TrackingTagId",
        $"TDID".as("UIID"),
        $"DataAggKey",
        $"DataAggValue",
        $"ConversionTime"
      )
      .as[DailyConversionDataRecord]
      .distinct

    val conversionXD = ConversionDataDailyTransform.addCrossDeviceTransform(
      transformedConvXD
      , xdSubsetDS
    )(prometheus)

    val resultDS = conversionNonXD.union(conversionXD)

    DailyConversionDataset.writePartition(resultDS, date)
  }
}