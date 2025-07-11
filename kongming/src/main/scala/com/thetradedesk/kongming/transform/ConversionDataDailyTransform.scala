package com.thetradedesk.kongming.transform

import com.thetradedesk.kongming.datasets._
import com.thetradedesk.kongming._
import com.thetradedesk.spark.TTDSparkContext.spark.implicits._
import com.thetradedesk.spark.sql.SQLFunctions._
import com.thetradedesk.spark.util.prometheus.PrometheusClient
import com.thetradedesk.streaming.records.rtb.conversiontracker.ConversionTrackerVerticaLoadRecord
import org.apache.spark.sql.Dataset
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._


final case class DailyTransformedConversionDataRecord(TrackingTagId: String,
                                                      TDID:String,
                                                      ConfigKey: String,
                                                      ConfigValue: String,
                                                      CrossDeviceConfidenceLevel: Option[Double],
                                                      ConversionTime: java.sql.Timestamp,
                                                      MonetaryValue: Option[BigDecimal],
                                                      MonetaryValueCurrency: Option[String],
                                                      CrossDeviceAttributionModelId: String
                                                     )


final case class DailyValidConversionDataRecord(
                                                 TDID: String,
                                                 TrackingTagId: String,
                                                 ConversionTime: java.sql.Timestamp,
                                                 AdvertiserId: String,
                                                 MonetaryValue: Option[BigDecimal],
                                                 MonetaryValueCurrency: Option[String]
                                               )

object ConversionDataDailyTransform {

  def dailyValidConversion(
                            conversionDS: Dataset[ConversionTrackerVerticaLoadRecord],
                            campaignDS: Dataset[CampaignRecord],
                            adGroupPolicy: Dataset[AdGroupPolicyRecord],
                          )(implicit prometheus: PrometheusClient): Dataset[DailyValidConversionDataRecord] = {

    task match {
      case "roas" => {
        // get custom revenue from attribution table
        val attributedEvent = AttributedEventDataSet().readDate(date)
        val attributedEventResult = AttributedEventResultDataSet().readDate(date)

        val filteredAttributedEvent = multiLevelJoinWithPolicy[AttributedEventRecord](attributedEvent, adGroupPolicy, joinType = "left_semi")
          .filter($"AttributedEventTypeId".isin(List("1", "2"): _*))
        val attributedCustomRevenue = attributedEventResult
          .filter($"AttributionMethodId".isin(List("0", "1"): _*))
          .join(filteredAttributedEvent.drop("ConversionTrackerId"),
            Seq("ConversionTrackerLogFileId", "ConversionTrackerIntId1", "ConversionTrackerIntId2",
              "AttributedEventLogFileId", "AttributedEventIntId1", "AttributedEventIntId2"))
          .join(broadcast(campaignDS.select($"CampaignId", $"CustomROASTypeId")), Seq("CampaignId"), "inner")
          .filter($"CustomROASTypeId" > lit(0))
          .withColumn("CustomRevenue", $"CustomRevenue".cast("decimal(10,2)"))
          .withColumn(
            "RevenueRank",
            // only retain the largest CustomRevenue for each conversion
            row_number().over(Window.partitionBy("ConversionTrackerId").orderBy($"CustomRevenue".desc)))
          .filter($"RevenueRank" === lit(1))
          .select(
            "ConversionTrackerLogFileId",
            "AdvertiserId",
            "ConversionTrackerId",
            "CustomRevenue" // [0, x)
          )

        // filter conversion data
        // TODO: do we need to consider multiple conversions during the day? Or we drop them? For now I am keeping all conv.
        // TODO: small number of trackingtagids are from event tracker. will need to reconsider that. less than .2% though.
        val conv = conversionDS
          .filter($"TDID".isNotNull && $"TDID" =!= "00000000-0000-0000-0000-000000000000")
          .withColumn("ConversionTime", when($"OfflineConversionTime".isNotNull, $"OfflineConversionTime").otherwise($"LogEntryTime"))
          .drop("LogEntryTime", "OfflineConversionTime")
          .withColumnRenamed("LogFileId", "ConversionTrackerLogFileId")
          .join(attributedCustomRevenue, Seq("ConversionTrackerLogFileId", "ConversionTrackerId", "AdvertiserId"), "left")
          .withColumn(
            "MonetaryValue",
            when($"CustomRevenue".isNull || $"CustomRevenue" === lit(0), $"MonetaryValue").otherwise($"CustomRevenue")
          )
          //TODO: check if this is the right way to add UID2 for conv data. Or there's better way to do it.

        conv.selectAs[DailyValidConversionDataRecord]
      }

      case _ => {
        val conv = conversionDS
          .filter($"TDID".isNotNull && $"TDID" =!= "00000000-0000-0000-0000-000000000000")
          .withColumn("ConversionTime", when($"OfflineConversionTime".isNotNull, $"OfflineConversionTime").otherwise($"LogEntryTime"))
          .drop("LogEntryTime", "OfflineConversionTime")
        //TODO: check if this is the right way to add UID2 for conv data. Or there's better way to do it.

        conv.selectAs[DailyValidConversionDataRecord]
      }
    }

  }


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
  def dailyTransform(
                     conversionDS: Dataset[ConversionTrackerVerticaLoadRecord],
                     ccrc: Dataset[CampaignConversionReportingColumnRecord],
                     adGroupPolicy: Dataset[AdGroupPolicyRecord],
                     adGroupMapping: Dataset[AdGroupPolicyMappingRecord],
                     campaignDS: Dataset[CampaignRecord])
                    (implicit prometheus: PrometheusClient): (Dataset[DailyTransformedConversionDataRecord], Dataset[IDRecord]) = {

    val conv = dailyValidConversion(conversionDS, campaignDS, adGroupPolicy)

    // get subset of trackingtag to process based on policy table and campaign setting
    // get the trackingtag weight in as well
    // output would be <aggkey, trackingtagid, weight>

    //1.process ccrc
    //TODO: we will move adding weight to later stage when we finished getting the positive labels.
    // Daily job on conversion will be just collection converison data.

    val customGoalTypeId = CustomGoalTypeId.get(task).get
    val includeInCustomGoal = IncludeInCustomGoal.get(task).get

    val ccrcPreProcessed = ccrc
      .join(broadcast(campaignDS.select($"CampaignId", col(customGoalTypeId))), Seq("CampaignId"), "inner")
      .filter((col(customGoalTypeId) === 0 && $"ReportingColumnId"===1) || (col(customGoalTypeId)>0 && col(includeInCustomGoal)))
      // will only use IAv2 and IAv2HH, other graphs will be replaced by IAv2
      .withColumn("CrossDeviceAttributionModelId",
        when(($"CrossDeviceAttributionModelId".isNotNull) && !($"CrossDeviceAttributionModelId".isin(List("IdentityAllianceWithHousehold", "IdentityAlliance"): _*)), lit("IdentityAlliance"))
          .otherwise($"CrossDeviceAttributionModelId")
      )

    val ccrcProcessed = task match {
      case "roas" => {
        ccrcPreProcessed.filter(col(customGoalTypeId) === 0 && $"ReportingColumnId"===1).union(
          ccrcPreProcessed.filter(col(customGoalTypeId)>0 && col(includeInCustomGoal))
            .filter((col(customGoalTypeId) === lit(1)) && ($"CustomROASWeight" =!= lit(0))
              or (col(customGoalTypeId) === lit(2)) && ($"CustomROASClickWeight" + $"CustomROASViewthroughWeight" =!= lit(0))
              or (col(customGoalTypeId) === lit(3)) && ($"CustomROASWeight" * ($"CustomROASClickWeight" + $"CustomROASViewthroughWeight") =!= lit(0))))
          .select("CampaignId","TrackingTagId", "AdvertiserId", "CrossDeviceAttributionModelId")
      }
      case _  => ccrcPreProcessed.select("CampaignId","TrackingTagId", "AdvertiserId", "CrossDeviceAttributionModelId")
    }

    val trackingTagWithSettings = adGroupPolicy
      .join(broadcast(adGroupMapping.select("ConfigKey", "ConfigValue", "CampaignId").distinct), Seq("ConfigKey", "ConfigValue"), "inner")
      .select("CampaignId", "ConfigKey", "ConfigValue", "CrossDeviceConfidenceLevel")
      .join(broadcast(ccrcProcessed), Seq("CampaignId"), "inner")
      .select("TrackingTagId", "ConfigKey", "ConfigValue", "CampaignId", "CrossDeviceConfidenceLevel", "AdvertiserId", "CrossDeviceAttributionModelId")
      // distinct to remove possible duplicate dataAggValue in the policy table
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
    val window = Window.partitionBy($"IdentityId", $"TrackingTagId", $"ConfigKey", $"ConfigValue").orderBy($"conversionTime".desc)
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
    val convWithDeviceIdDS = convWithPersonIdDS.join(xdDS, $"IdentityId"===$"PersonId", "inner")
      .union(convWithPersonIdDS.join(xdDS, $"IdentityId" === $"HouseholdID", "inner"))
      //only include additional IDs added by cross device graph in this case. The raw conversions will be included outside this function.
      .filter($"TDID"=!=$"uiid")
      .withColumnRenamed("uiid", "UIID")
      .filter($"CrossDeviceConfidenceLevel"<=$"score")
      .drop("score")
      .selectAs[DailyConversionDataRecord]
      .distinct

    convWithDeviceIdDS
  }

}
