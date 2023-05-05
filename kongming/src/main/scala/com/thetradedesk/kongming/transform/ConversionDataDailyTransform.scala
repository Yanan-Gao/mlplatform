package com.thetradedesk.kongming.transform

import com.thetradedesk.kongming.datasets._
import com.thetradedesk.kongming.IdentityHouseholdUnmatchedToken
import com.thetradedesk.spark.TTDSparkContext.spark.implicits._
import com.thetradedesk.spark.sql.SQLFunctions._
import com.thetradedesk.spark.util.prometheus.PrometheusClient
import com.thetradedesk.streaming.records.rtb.conversiontracker.ConversionTrackerVerticaLoadRecord
import org.apache.spark.sql.Dataset
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._


final case class DailyTransformedConversionDataRecord(TrackingTagId: String,
                                                      TDID:String,
                                                      DataAggKey: String,
                                                      DataAggValue: String,
                                                      //CrossDeviceUsage: Boolean,
                                                      CrossDeviceConfidenceLevel: Option[Double],
                                                      //Weight: Double,
                                                      ConversionTime: java.sql.Timestamp,
                                                      CrossDeviceAttributionModelId: String
                                                     )


object ConversionDataDailyTransform {


  /**
   *  dailyTransform will take in conversion data and configurations and output a filtered dataset with conversion data
   *  as well as a dataset with all the converted Ids for assisting graph operations.
   * @param conversionDS conversion data
   * @param ccrc conversion reporting column data
   * @param adGroupPolicy adgroup policy data
   * @param adGroupDS adgroup metadata
   * @param campaignDS campaign metadata
   * @return convResult conversion data expanded by DataAggValue and filtered by policy;
   *         distinctId unique ids in conversion dataset for cross device considerations
   */
  def dailyTransform(conversionDS: Dataset[ConversionTrackerVerticaLoadRecord],
                     ccrc: Dataset[CampaignConversionReportingColumnRecord],
                     adGroupPolicy: Dataset[AdGroupPolicyRecord],
                     adGroupDS: Dataset[AdGroupRecord],
                     campaignDS: Dataset[CampaignRecord])
                    (implicit prometheus: PrometheusClient): (Dataset[DailyTransformedConversionDataRecord], Dataset[IDRecord]) = {
    // filter conversion data
    // TODO: do we need to consider multiple conversions during the day? Or we drop them? For now I am keeping all conv.
    // TODO: small number of trackingtagids are from event tracker. will need to reconsider that. less than .2% though.
    val conv = conversionDS
      .filter($"TDID".isNotNull && $"TDID" =!= "00000000-0000-0000-0000-000000000000")
      .withColumn("ConversionTime", when($"OfflineConversionTime".isNotNull, $"OfflineConversionTime").otherwise($"LogEntryTime"))
      .drop("LogEntryTime", "OfflineConversionTime")
      //TODO: check if this is the right way to add UID2 for conv data. Or there's better way to do it.
      .select($"TDID",//coalesce($"TDID",$"UnifiedId2").as("TDID"),
        $"TrackingTagId",
        $"ConversionTime",
        $"AdvertiserId"
      )

    // get subset of trackingtag to process based on policy table and campaign setting
    // get the trackingtag weight in as well
    // output would be <aggkey, trackingtagid, weight>

    //1.process ccrc
    //TODO: we will move adding weight to later stage when we finished getting the positive labels.
    // Daily job on conversion will be just collection converison data.
    val ccrcProcessed = ccrc
      .join(broadcast(campaignDS.select($"CampaignId", $"CustomCPATypeId")), Seq("CampaignId"), "left")
      .filter(($"CustomCPATypeId"===0 && $"ReportingColumnId"===1) || ($"CustomCPATypeId">0 && $"IncludeInCustomCPA") )
      // will only use IAv2 and IAv2HH, other graphs will be replaced by IAv2
      .withColumn("CrossDeviceAttributionModelId",
        when(($"CrossDeviceAttributionModelId".isNotNull) && !($"CrossDeviceAttributionModelId".isin(List("IdentityAllianceWithHousehold", "IdentityAlliance"): _*)), lit("IdentityAlliance"))
          .otherwise($"CrossDeviceAttributionModelId")
      )
      .select("CampaignId","TrackingTagId", "AdvertiserId", "CrossDeviceAttributionModelId")

    val trackingTagWithSettings = adGroupPolicy
      .join(broadcast(adGroupDS), adGroupPolicy("ConfigValue")===adGroupDS("AdGroupId"), "inner")
      .select("CampaignId","DataAggKey","DataAggValue","CrossDeviceConfidenceLevel")
      .join(broadcast(ccrcProcessed), Seq("CampaignId"), "inner")
      .select( "TrackingTagId","DataAggKey","DataAggValue","CrossDeviceConfidenceLevel","AdvertiserId", "CrossDeviceAttributionModelId")//, "Weight")
      //distinct to remove possible duplicate dataAggValue in the policy table
      .distinct

    val convResult = conv.join(broadcast(trackingTagWithSettings), Seq("TrackingTagId", "AdvertiserId"), "inner").selectAs[DailyTransformedConversionDataRecord].cache()
    val distinctId = convResult.select($"TDID".as("uiid")).distinct.selectAs[IDRecord]

    (convResult, distinctId)
  }

  /**
   * add cross device to conversion dataset according to policy set.
   * @param transformedConvDS filtered conversion dataset with True setting on xd policy
   * @param xdDS cross device dataset
   * @return expanded conversion dataset.
   */
  def addCrossDeviceTransform(
                              transformedConvDS: Dataset[DailyTransformedConversionDataRecord],
                              xdDS: Dataset[CrossDeviceGraphRecord]
                            )
                            (implicit prometheus: PrometheusClient): Dataset[DailyConversionDataRecord] = {
    //IdentityId would be personId for iav2 usage and householdId for iav2hh usage
    val window = Window.partitionBy($"IdentityId", $"TrackingTagId", $"DataAggKey", $"DataAggValue").orderBy($"conversionTime".desc)
    val convWithPersonIdDS = transformedConvDS
      .join(xdDS, transformedConvDS("TDID")===xdDS("uiid"),"inner")
      .withColumn("IdentityId",
        when($"CrossDeviceAttributionModelId"===lit("IdentityAllianceWithHousehold"), $"HouseholdID")
        .otherwise($"PersonId")
      )
      .filter($"IdentityId"=!=lit(IdentityHouseholdUnmatchedToken))
      .drop("uiid","score", "PersonId","HouseholdID")
      .withColumn("rank", rank().over(window))
      .filter($"rank"<=3)//TODO: may need to revisit to see if this is reasonable or need some modification.
      .drop("rank") //alleviate possible inflation on conversion due to multiple conversions belongs to the same person.

    // keep the latest conversion if there multiple on the same person
    val convWithDeviceIdDS =
      ( convWithPersonIdDS.join(xdDS, $"IdentityId"===$"PersonId", "inner")
        .union(convWithPersonIdDS.join(xdDS, $"IdentityId"===$"HouseholdID", "inner") )
        )
      //only include additional IDs added by cross device graph in this case. The raw conversions will be included outside this function.
      .filter($"TDID"=!=$"uiid")
      .select(
        $"TrackingTagId",
        $"DataAggKey",
        $"DataAggValue",
        //$"CrossDeviceUsage",
        //$"CrossDeviceAttributionModelId",
        $"CrossDeviceConfidenceLevel",
        //, $"Weight"
        $"ConversionTime",
        $"uiid".as("UIID"),
        $"score" )
      .filter($"CrossDeviceConfidenceLevel"<=$"score")
      .drop("score")
      .selectAs[DailyConversionDataRecord]
      .distinct

    convWithDeviceIdDS
  }

}
