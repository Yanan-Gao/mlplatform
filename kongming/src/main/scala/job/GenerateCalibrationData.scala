package job

import com.thetradedesk.geronimo.shared.encodeStringIdUdf
import com.thetradedesk.kongming.datasets._
import com.thetradedesk.kongming.features.Features.encodeDatasetForCBuffer
import com.thetradedesk.kongming._
import com.thetradedesk.spark.TTDSparkContext.spark.implicits._
import com.thetradedesk.spark.sql.SQLFunctions._
import com.thetradedesk.spark.util.TTDConfig.config
import org.apache.spark.sql.Dataset
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._

import java.time.format.DateTimeFormatter


object GenerateCalibrationData extends KongmingBaseJob {

  override def jobName: String = "GenerateCalibrationData"

  val isIncCVR = config.getBoolean("isIncCVR", true)
  val cvrChangeCap = config.getInt("cvrChangeCap", 20)
  val cvrImpressionMin = config.getInt("cvrImpressionMin", 1000)
  val defaultCvrPercentile = config.getDouble("defaultCvrPercentile", 0.75)
  val partitionCount = config.getInt("partitionCount", partCount.SampledImpressionForIsoReg)
  val saveDataAsCSV = config.getBoolean("saveDataAsCSV", true)
  val saveDataAsCBuffer = config.getBoolean("saveDataAsCBuffer", true)

  private def sampledImpressionForCalibration(attributedData: Dataset[OutOfSampleAttributionRecord], adgroupCampaignCVR: Dataset[_]): Dataset[OutOfSampleAttributionRecord] = {
    val attDataToSample = attributedData.join(
      broadcast(
        adgroupCampaignCVR
          .filter(($"AdGroupConversions" > lit(Config.IsotonicPosCntMin)) || ($"CampaignConversions" > lit(Config.IsotonicPosCntMin*2)))
          .select("AdGroupIdStr", "CampaignNegatives")),
      Seq("AdGroupIdStr"),
      "inner"
    )

    attDataToSample.withColumn(
        "NegSampleRate", least(lit(Config.IsotonicNegSampleRateMax), lit(Config.IsotonicNegCntMax) / $"CampaignNegatives")
      ).filter(($"Target" === 0 && rand(seed = samplingSeed) < $"NegSampleRate") || ($"Target" === 1))
      .withColumn("Weight", when($"Target" === lit(0), $"Weight" / $"NegSampleRate").otherwise($"Weight").cast("float"))
      .drop("NegSampleRate")
      .selectAs[OutOfSampleAttributionRecord]

  }

  override def runTransform(args: Array[String]): Array[(String, Long)] = {

    // 1. Load attributed data
    val NDelayDays = Config.CalibrationImpLookBack + Config.CalibrationAttLookBack
    val attributedData = OutOfSampleAttributionDataset(delayNDays=NDelayDays).readPartition(date.minusDays(NDelayDays))

    val dateString = date.format(DateTimeFormatter.ofPattern("yyyyMMdd"))

    // 2. Calculate and save campaign cvr

    val campaignWin = Window.partitionBy("CampaignIdStr")

    val adgroupCampaignCVR = attributedData.groupBy("AdGroupIdStr", "CampaignIdStr").agg(
        sum(lit(1)).alias("AdGroupImpressions"),
        sum($"Weight" * $"Target").alias("AdGroupConversions"),
      ).withColumn("AdGroupCVR", $"AdGroupConversions"/$"AdGroupImpressions")
      .withColumn("CampaignConversions", sum($"AdGroupConversions").over(campaignWin))
      .withColumn("CampaignImpressions", sum($"AdGroupImpressions").over(campaignWin))
      .withColumn("CampaignNegatives", least($"CampaignImpressions" - $"CampaignConversions", lit(0)))
      .withColumn("CampaignCVR", $"CampaignConversions" / $"CampaignImpressions")
      .cache()

    val campaignCVR = adgroupCampaignCVR.select($"CampaignIdStr".alias("Id"), $"CampaignCVR".alias("CVR"))
      .filter(col("CampaignImpressions") > cvrImpressionMin)
      .distinct()
      .withColumn("Level", lit("CampaignId"))
      .withColumn("LastUpdateDate", lit(dateString))
      .withColumn("CVRSmooth", col("CVR"))
      .selectAs[CvrForScalingRecord]

    val defaultCVR = campaignCVR.selectExpr(s"percentile_approx(CVR, $defaultCvrPercentile) as default").first().getAs[Double]("default")
    val defaultRow = Seq(CvrForScalingRecord("Default", "default", defaultCVR, defaultCVR, dateString)).toDF().selectAs[CvrForScalingRecord]
    val campaignCVRWithDefault = campaignCVR.union(defaultRow)

    val allCampaignCVR = if (isIncCVR) {
      val previousCVR = CvrForScalingDataset().readLatestPartitionUpTo(date.minusDays(1), isInclusive = true)
      val defaultCVRPrevious = previousCVR.filter(col("Level") === lit("Default")).first().CVRSmooth

      previousCVR.as("Prev")
        .join(campaignCVRWithDefault.as("Curr"), Seq("Level", "Id"), joinType = "outer")
        .select(
          col("Level"),
          col("Id"),
          col("Curr.CVR").alias("CVRCurr"),
          coalesce(col("Prev.CVRSmooth"), lit(defaultCVRPrevious)).alias("CVRSmoothPrev"),
          coalesce(col("Curr.CVR"), col("Prev.CVR")).alias("CVR"),
          coalesce(col("Curr.LastUpdateDate"), col("Prev.LastUpdateDate")).alias("LastUpdateDate"),
        ).withColumn("CVRSmooth",
          when(col("CVRCurr").isNull, col("CVRSmoothPrev"))
            .otherwise(sqrt(col("CVRCurr") * col("CVRSmoothPrev")))
        ).withColumn("CVRSmooth",
          least(lit(cvrChangeCap) * col("CVRSmoothPrev"), greatest(col("CVRSmooth"), lit(1/cvrChangeCap) * col("CVRSmoothPrev"))))
        .filter((col("CVR") > lit(0)) && (col("CVR") < lit(1)))
        .selectAs[CvrForScalingRecord]
    } else {
      campaignCVRWithDefault
    }

    // 3. Sample adgroups that satisfy IsotonicPosCntMin for adgroup and campaign
    val sampledImpression = sampledImpressionForCalibration(attributedData, adgroupCampaignCVR)

    var isotonicSampledRows = ("", 0L)

    if (saveDataAsCSV) {
      isotonicSampledRows = SampledImpressionForIsotonicRegDataset().writePartition(
        sampledImpression, date, Some(partitionCount)
      )
    }

    if (saveDataAsCBuffer) {
      val contextualColNames = (0 to 30).map(i => "ContextualCategoriesTier1_Column" + i.toString)
      val audienceIdColNames = (0 to 29).map(i => "AudienceId_Column" + i.toString)
      val userDataColNames = (0 to 140).map(i => "UserData_Column" + i.toString)
      // Temporary hack to make it work with the flattened dataset. This can be simplified once we remove the Csv datasets
      val sampledArrayImpression = sampledImpression
        .withColumn("ContextualCategoriesTier1", array(contextualColNames.head, contextualColNames.tail: _*))
        .withColumn("AudienceId", array(audienceIdColNames.head, audienceIdColNames.tail: _*))
        .withColumn("UserData", array(userDataColNames.head, userDataColNames.tail: _*))
        .withColumn("AdGroupIdEncoded", encodeStringIdUdf('AdGroupIdStr))
        .withColumn("CampaignIdEncoded", encodeStringIdUdf('CampaignIdStr))
        .withColumn("AdvertiserIdEncoded", encodeStringIdUdf('AdvertiserIdStr))
        .withColumn("UserTargetingDataIds", typedLit(Seq()))
        .withColumn("IdType", lit(0))
        .withColumn("IdCount", lit(0))
        .withColumn("UserAgeInDays", lit(0f))
      isotonicSampledRows = ArraySampledImpressionForIsotonicRegDataset().writePartition(encodeDatasetForCBuffer[ArrayOutOfSampleAttributionRecord](sampledArrayImpression), date, None, partitionCount, evalBatchSize)
    }

    val cvrScalingRows = CvrForScalingDataset().writePartition(
      allCampaignCVR, date, Some(partCount.CvrRescaling)
    )
    Array(isotonicSampledRows, cvrScalingRows)

  }
}

