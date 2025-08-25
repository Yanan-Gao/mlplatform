package job

import com.thetradedesk.geronimo.shared.loadParquetData
import com.thetradedesk.geronimo.shared.schemas.BidFeedbackDataset
import com.thetradedesk.kongming._
import com.thetradedesk.kongming.datasets._
import com.thetradedesk.kongming.features.Features.{aliasedModelFeatureCols, encodeDatasetForCBuffer, seqDirectFields, seqHashFields}
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
import com.thetradedesk.spark.datasets.core.DefaultTimeFormatStrings

import java.time.LocalDate

object OutOfSampleAttributionSetGenerator extends KongmingBaseJob {

  override def jobName: String = "OutOfSampleAttributionSetGenerator"
  val applyCustomCPACountAsWeight = config.getBoolean("applyCustomCPACountAsWeight", true)
  val isExactAttributionLookBack = config.getBoolean("isExactAttributionLookBack", true)
  val isSampledNegativeWeight = config.getBoolean("isSampledNegativeWeight", false)

  val saveTrainingDataAsCSV = config.getBoolean("saveTrainingDataAsCSV", true)
  val saveTrainingDataAsCBuffer = config.getBoolean("saveTrainingDataAsCBuffer", true)

  val OOSPartitionCountFactor = config.getInt("OOSPartitionCountFactor", 1)

  override def runTransform(args: Array[String]): Array[(String, Long)] = {

    val delayNDays = Config.ImpressionLookBack + Config.AttributionLookBack
    val scoreDate = date.minusDays(delayNDays)

    val attributionSet = generateAttributionSet(scoreDate)(getPrometheus)

    val parquetSelectionTabular = attributionSet.columns.map { c => col(c) }.toArray ++ aliasedModelFeatureCols(seqDirectFields ++ seqHashFields)

    var datasetRows = Array.fill(2)("", 0L)

    if (saveTrainingDataAsCSV) {
      val TrackedAttributionSet = attributionSet.filter($"IsTracked" === lit(1)).select(parquetSelectionTabular: _*).selectAs[OldOutOfSampleAttributionRecord]
      val UntrackedAttributionSet = attributionSet.filter($"IsTracked" =!= lit(1)).select(parquetSelectionTabular: _*).selectAs[OldOutOfSampleAttributionRecord]
      OldOutOfSampleAttributionDataset(delayNDays).writePartition(TrackedAttributionSet, scoreDate, "tracked", Some(200))
      OldOutOfSampleAttributionDataset(delayNDays).writePartition(UntrackedAttributionSet, scoreDate, "untracked", Some(200))

      val TrackedAttributionSetWithUserData = attributionSet.filter($"IsTracked" === lit(1)).select(parquetSelectionTabular: _*).selectAs[OutOfSampleAttributionRecord]
      val UntrackedAttributionSetWithUserData = attributionSet.filter($"IsTracked" =!= lit(1)).select(parquetSelectionTabular: _*).selectAs[OutOfSampleAttributionRecord]

      OutOfSampleAttributionDatasetDeprecated(delayNDays).writePartition(TrackedAttributionSetWithUserData, scoreDate, "tracked", Some(partCount.OOSTrackedPerDayDelay * delayNDays))
      OutOfSampleAttributionDatasetDeprecated(delayNDays).writePartition(UntrackedAttributionSetWithUserData, scoreDate, "untracked", Some(partCount.OOSUntrackedPerDayDelay * delayNDays))
      val numTrackedRows = OutOfSampleAttributionDataset(delayNDays).writePartition(TrackedAttributionSetWithUserData, scoreDate, "tracked", Some(partCount.OOSTrackedPerDayDelay * delayNDays))
      val numUntrackedRows = OutOfSampleAttributionDataset(delayNDays).writePartition(UntrackedAttributionSetWithUserData, scoreDate, "untracked", Some(partCount.OOSUntrackedPerDayDelay * delayNDays))
      datasetRows = Array(numTrackedRows, numUntrackedRows)
    }

    if (saveTrainingDataAsCBuffer) {
      val TrackedAttributionSetWithUserData = attributionSet.filter($"IsTracked" === lit(1))
      val UntrackedAttributionSetWithUserData = attributionSet.filter($"IsTracked" =!= lit(1))

      val numTrackedRows = ArrayOutOfSampleAttributionDataset(delayNDays).writePartition(encodeDatasetForCBuffer[ArrayOutOfSampleAttributionRecord](TrackedAttributionSetWithUserData), scoreDate, Some("tracked"), partCount.OOSTrackedPerDayDelay * delayNDays * OOSPartitionCountFactor, evalBatchSize)
      val numUntrackedRows = ArrayOutOfSampleAttributionDataset(delayNDays).writePartition(encodeDatasetForCBuffer[ArrayOutOfSampleAttributionRecord](UntrackedAttributionSetWithUserData), scoreDate, Some("untracked"), partCount.OOSUntrackedPerDayDelay * delayNDays * OOSPartitionCountFactor, evalBatchSize)
      datasetRows = Array(numTrackedRows, numUntrackedRows)
    }

    datasetRows

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

  def generateAttributionSet(scoreDate: LocalDate)(implicit prometheus: PrometheusClient): Dataset[UnionOutOfSampleAttributionRecord] = {
    val adGroupPolicyMapping = AdGroupPolicyMappingDataset().readDate(scoreDate)
    val campaignList = adGroupPolicyMapping.select("CampaignId").distinct().cache()

    val rawOOS = (1 to Config.ImpressionLookBack).map(i => {
      val ImpDate = scoreDate.plusDays(i)
      // Attr [T-ConvLB, T]
      val AttrDates = (ImpDate.toEpochDay to date.toEpochDay).map(LocalDate.ofEpochDay)
      val dailyImp = OldDailyOfflineScoringParquetDataset().readDate(ImpDate)
      val impressionsToScore = dailyImp.withColumnRenamed("AdGroupId", "AdGroupIdInt").withColumnRenamed("AdGroupIdStr", "AdGroupId")
        .withColumnRenamed("CampaignId", "CampaignIdInt").withColumnRenamed("CampaignIdStr", "CampaignId")
        .withColumnRenamed("AdvertiserId", "AdvertiserIdInt").withColumnRenamed("AdvertiserIdStr", "AdvertiserId")
        .join(broadcast(campaignList), Seq("CampaignId"), "inner").selectAs[BidRequestsWithAdGroupId]

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

    val rawOOSFiltered =  
      rawOOS.join(broadcast(campaignsWithPosSamples), Seq("CampaignId"), "left_semi")
      .withColumn("AdGroupIdEncoded", encodeStringIdUdf('AdGroupIdStr))
      .withColumn("CampaignIdEncoded", encodeStringIdUdf('CampaignIdStr))
      .withColumn("AdvertiserIdEncoded", encodeStringIdUdf('AdvertiserIdStr))
      .withColumn("UserDataOptIn",lit(2)) // hashmod 1 ->2
      .withColumn("sin_hour_week", $"sin_hour_week".cast("float"))
      .withColumn("cos_hour_week", $"cos_hour_week".cast("float"))
      .withColumn("sin_hour_day", $"sin_hour_day".cast("float"))
      .withColumn("cos_hour_day", $"cos_hour_day".cast("float"))
      .withColumn("sin_minute_hour", $"sin_minute_hour".cast("float"))
      .withColumn("cos_minute_hour", $"cos_minute_hour".cast("float"))
      .withColumn("latitude", $"latitude".cast("float"))
      .withColumn("longitude", $"longitude".cast("float"))
      .withColumn("UserDataLength", $"UserDataLength".cast("float"))
      .withColumn("ContextualCategoryLengthTier1", $"ContextualCategoryLengthTier1".cast("float"))
      .withColumn("UserAgeInDays", $"UserAgeInDays".cast("float"))
      .withColumn("Target", $"Target".cast("float"))
      .withColumn("Weight", $"Weight".cast("float"))
      .withColumn("Revenue", $"Revenue".cast("float"))
      .selectAs[UnionOutOfSampleAttributionRecord]
      .cache()

    rawOOSFiltered
  }
}
