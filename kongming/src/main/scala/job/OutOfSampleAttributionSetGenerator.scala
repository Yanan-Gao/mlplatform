package job

import com.thetradedesk.geronimo.shared.loadParquetData
import com.thetradedesk.geronimo.shared.schemas.BidFeedbackDataset
import com.thetradedesk.kongming._
import com.thetradedesk.kongming.datasets._
import com.thetradedesk.kongming.features.Features.{aliasedModelFeatureCols, seqDirectFields, seqHashFields}
import com.thetradedesk.kongming.transform.TrainSetTransformation.getValidTrackingTags
import com.thetradedesk.kongming.transform._
import com.thetradedesk.spark.TTDSparkContext.spark.implicits._
import com.thetradedesk.spark.datasets.sources.datalake.ClickTrackerDataSetV5
import com.thetradedesk.spark.sql.SQLFunctions._
import com.thetradedesk.spark.util.TTDConfig.{config, defaultCloudProvider}
import com.thetradedesk.spark.util.prometheus.PrometheusClient
import org.apache.spark.sql.Dataset
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._
import com.thetradedesk.kongming.transform.TrainSetTransformation._
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.types.DoubleType
import com.thetradedesk.spark.datasets.core.DefaultTimeFormatStrings

import java.time.LocalDate

object OutOfSampleAttributionSetGenerator extends KongmingBaseJob {

  override def jobName: String = "OutOfSampleAttributionSetGenerator"
  val applyCustomCPACountAsWeight = config.getBoolean("applyCustomCPACountAsWeight", true)
  val isExactAttributionLookBack = config.getBoolean("isExactAttributionLookBack", true)
  val isSampledNegativeWeight = config.getBoolean("isSampledNegativeWeight", false)

  override def runTransform(args: Array[String]): Array[(String, Long)] = {

    val delayNDays = Config.ImpressionLookBack + Config.AttributionLookBack
    val scoreDate = date.minusDays(delayNDays)

    val attributionSet = generateAttributionSet(scoreDate)(getPrometheus)

    val TrackedAttributionSetWithUserData = attributionSet._1.filter($"IsTracked" === lit(1)).selectAs[OutOfSampleAttributionRecord]
    val UntrackedAttributionSetWithUserData = attributionSet._1.filter($"IsTracked" =!= lit(1)).selectAs[OutOfSampleAttributionRecord]

    val TrackedAttributionSet = attributionSet._2.filter($"IsTracked" === lit(1)).selectAs[OldOutOfSampleAttributionRecord]
    val UntrackedAttributionSet = attributionSet._2.filter($"IsTracked" =!= lit(1)).selectAs[OldOutOfSampleAttributionRecord]

    val numTrackedRows = OutOfSampleAttributionDataset(delayNDays).writePartition(TrackedAttributionSetWithUserData, scoreDate, "tracked", Some(partCount.OOSTracked))
    val numUntrackedRows = OutOfSampleAttributionDataset(delayNDays).writePartition(UntrackedAttributionSetWithUserData, scoreDate, "untracked", Some(partCount.OOSUntracked))

    OldOutOfSampleAttributionDataset(delayNDays).writePartition(TrackedAttributionSet, scoreDate, "tracked", Some(200))
    OldOutOfSampleAttributionDataset(delayNDays).writePartition(UntrackedAttributionSet, scoreDate, "untracked", Some(200))

    Array(numTrackedRows, numUntrackedRows)

  }

  final case class BidRequestsWithAdGroupId(
                                               BidRequestIdStr: String,
                                               AdGroupId: String
                                             )


  final case class BidRequestsWithConfigValue(
                                                  BidRequestIdStr: String,
                                                  ConfigKey: String,
                                                  ConfigValue: String
                                                  )

  def generateAttributionSet(scoreDate: LocalDate)(implicit prometheus: PrometheusClient):
  (Dataset[OutOfSampleAttributionRecord], Dataset[OldOutOfSampleAttributionRecord]) = {
    val adGroupPolicy = AdGroupPolicyDataset().readDate(scoreDate)
    val adGroupPolicyMapping = AdGroupPolicyMappingDataset().readDate(scoreDate)
    val policy = getMinimalPolicy(adGroupPolicy, adGroupPolicyMapping).cache()

    val rawOOS = (1 to Config.ImpressionLookBack).map(i => {
      val ImpDate = scoreDate.plusDays(i)
      // Attr [T-ConvLB, T]
      val AttrDates = (ImpDate.toEpochDay to date.toEpochDay).map(LocalDate.ofEpochDay)
      val dailyImp = OldDailyOfflineScoringDataset().readDate(ImpDate)
      val impressionsToScore = multiLevelJoinWithPolicy[BidRequestsWithAdGroupId](
        dailyImp.withColumnRenamed("AdGroupId", "AdGroupIdInt").withColumnRenamed("AdGroupIdStr", "AdGroupId")
          .withColumnRenamed("CampaignId", "CampaignIdInt").withColumnRenamed("CampaignIdStr", "CampaignId")
          .withColumnRenamed("AdvertiserId", "AdvertiserIdInt").withColumnRenamed("AdvertiserIdStr", "AdvertiserId"),
        policy,
        "inner")
      val attr = AttrDates.map(dt => {
        DailyAttributionEventsDataset().readPartition(dt, ImpDate.format(DefaultTimeFormatStrings.dateTimeFormatter))
      })
      .reduce(_.union(_))

      val campaignDS = CampaignDataSet().readLatestPartitionUpTo(ImpDate, true)

      val selectedAttr =
        if (isExactAttributionLookBack) {
          attr.filter(
            unix_timestamp($"ConversionTrackerLogEntryTime") -
              unix_timestamp($"AttributedEventLogEntryTime") <= Config.AttributionLookBack * 86400
          )
        } else {
          attr
        }

      val filteredAttr = campaignDS.select($"CampaignId", $"CustomCPATypeId").join(
        selectedAttr, Seq("CampaignId"), "inner"
      )

      val filteredAttrWithWeight = 
        if(applyCustomCPACountAsWeight && task=="cpa"){
          filteredAttr
          .withColumn("Weight", when($"CustomCPATypeId"===0, 1).otherwise($"CustomCPACount"))
        }else{
          filteredAttr
          .withColumn("Weight", lit(1))
        }

      val NegPosRatio = Config.OosNegPosRatio
      val win = Window.partitionBy("AdGroupId")

      val impWithAttr = impressionsToScore.join(broadcast(filteredAttrWithWeight.select($"BidRequestId".alias("BidRequestIdStr"), $"Target", $"Revenue", $"Weight")), Seq("BidRequestIdStr"), "left")
        .withColumn("Target", coalesce('Target, lit(0)))
        .withColumn("Revenue", coalesce('Revenue, lit(0)))
        .withColumn("Weight", coalesce('Weight,lit(1)))
        .withColumn("AdGroupPosCount", sum($"Target").over(win))
        .withColumn("AdGroupNegCount", sum(lit(1) - $"Target").over(win))
        .withColumn("SamplingRate", $"AdGroupPosCount" * lit(NegPosRatio) / $"AdGroupNegCount")
        .withColumn("SamplingRate", when($"Target" === lit(1), 1).otherwise($"SamplingRate"))
        .filter(rand(seed = samplingSeed) <= $"SamplingRate")

      val impWithAttrWeight = if (isSampledNegativeWeight) {
        impWithAttr.withColumn("Weight", $"Weight" / $"SamplingRate")
      } else { impWithAttr }

      val impWithAttrFeature = impWithAttrWeight.drop("AdGroupId", "AdGroupPosCount", "AdGroupNegCount", "SamplingRate")
        .join(dailyImp, Seq("BidRequestIdStr"), "inner")

      dailyImp.unpersist()

      impWithAttrFeature
    }).reduce(_.union(_)).cache()

    val campaignsWithPosSamples = rawOOS.filter('Target === lit(1)).select('CampaignId).distinct
    val parquetSelectionTabular = rawOOS.columns.map { c => col(c) }.toArray ++ aliasedModelFeatureCols(seqDirectFields ++ seqHashFields)

    val rawOOSFiltered =  
      rawOOS.join(broadcast(campaignsWithPosSamples), Seq("CampaignId"), "left_semi")
      .select(parquetSelectionTabular: _*)
      .cache()

    val rawOOSWithUserData = rawOOSFiltered
      .withColumn("UserDataOptIn",lit(2)) // hashmod 1 ->2
      .selectAs[OutOfSampleAttributionRecord]

    val rawOOSdata = rawOOSFiltered.selectAs[OldOutOfSampleAttributionRecord]

    (rawOOSWithUserData, rawOOSdata)
  }
}
