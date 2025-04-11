package job

import com.thetradedesk.geronimo.shared.encodeStringIdUdf
import com.thetradedesk.kongming._
import com.thetradedesk.kongming.datasets._
import com.thetradedesk.kongming.features.Features._
import com.thetradedesk.spark.TTDSparkContext.spark.implicits._
import com.thetradedesk.spark.datasets.core.DefaultTimeFormatStrings
import com.thetradedesk.spark.sql.SQLFunctions._
import com.thetradedesk.spark.util.TTDConfig.config
import com.thetradedesk.spark.util.prometheus.PrometheusClient
import org.apache.spark.sql.Dataset
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions.{lit, _}
import com.thetradedesk.kongming.transform.TrainSetTransformation._
import org.apache.spark.sql.types.DoubleType
import java.time.LocalDate

object GenerateTrainSetLastTouch extends KongmingBaseJob {

  override def jobName: String = "GenerateTrainSetLastTouch"

  val incTrain = config.getBoolean("incTrain", false)
  val trainRatio = config.getDouble("trainRatio", 0.8)
  val desiredNegOverPos = config.getInt(path = "desiredPosOverNeg", 9)
  val maxPositiveCount = config.getInt(path="maxPositiveCount", 500000)
  val conversionLookback = config.getInt("conversionLookback", 15)

  val saveTrainingDataAsCSV = config.getBoolean("saveTrainingDataAsCSV", true)
  val saveTrainingDataAsCBuffer = config.getBoolean("saveTrainingDataAsCBuffer", true)
  val addBidRequestId = config.getBoolean("addBidRequestId", false)

  val trainSetPartitionCount = config.getInt("trainSetPartitionCount", partCount.trainSet)
  val valSetPartitionCount = config.getInt("valSetPartitionCount", partCount.valSet)

  /*
  Params for func generateWeightForNegative
  The default Offset and Coefficient is derived from f = 1 - a * exp(b*x) that fits the overall distribution of positive samples
  Need to be updated if the distribution change dramatically
  */
  val negativeWeightMethod = config.getString("negativeWeightMethod", "PostDistVar")
  val negativeWeightOffset = config.getDouble("negativeWeightOffset", 0.7)
  val negativeWeightCoefficient = config.getDouble("negativeWeightCoef", -0.33)
  val negativeWeightThreshold = config.getInt("negativeWeightThreshold", 3000)

  val applyPositiveReweight = config.getBoolean("applyPositiveReweight", true)
  val applyNegativeReweight = config.getBoolean("applyNegativeReweight", true)
  val applyPosNegBalance = config.getBoolean("applyPosNegBalance",true)

  val maxConvPerBR = config.getInt("maxConvPerBR", 5)

  def generateTrainSet()(implicit prometheus: PrometheusClient): Dataset[UserDataValidationDataForModelTrainingRecord] = {
    val startDate = date.minusDays(conversionLookback)
    val win = Window.partitionBy($"CampaignIdEncoded", $"AdGroupIdEncoded")

    // Imp [T-ConvLB, T]
    val sampledImpressionWithAttribution = (0 to conversionLookback).map(i => {
      val ImpDate = startDate.plusDays(i)
      // Attr [T-ConvLB, T]
      val AttrDates = 
        if(incTrain){
          (date.toEpochDay to date.toEpochDay).map(LocalDate.ofEpochDay)
        }else{
          (ImpDate.toEpochDay to date.toEpochDay).map(LocalDate.ofEpochDay)
        }

      val dailyImp = OldDailyOfflineScoringDataset().readDate(ImpDate)
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

      val attr = AttrDates.map(dt => {
        DailyAttributionEventsDataset().readPartition(dt, ImpDate.format(DefaultTimeFormatStrings.dateTimeFormatter))
        }).reduce(_.union(_))
        .withColumn("Revenue", $"Revenue".cast("float"))

      val winBR = Window.partitionBy("BidRequestId")
      val sampledAttr = attr.withColumn("BRConv", count($"Target").over(winBR))
        .withColumn("ratio", lit(maxConvPerBR) / $"BRConv")
        .withColumn("rand", rand(seed = samplingSeed))
        .filter($"rand" <= $"ratio")
        .drop("BRConv", "ratio", "rand")

      val campaignDS = CampaignDataSet().readLatestPartitionUpTo(ImpDate, true)
      val sampledAttrWithWeight =
        campaignDS.select($"CampaignId", $"CustomCPATypeId").join(broadcast(sampledAttr), Seq("CampaignId"), "inner")
        .withColumn("CPACountWeight", when($"CustomCPATypeId"===0, 1).otherwise($"CustomCPACount".cast("float")))

      val impWithAttr = dailyImp.join(broadcast(sampledAttrWithWeight.select($"BidRequestId".alias("BidRequestIdStr"), $"Target", $"Revenue",$"CPACountWeight",$"ConversionTrackerLogEntryTime")), Seq("BidRequestIdStr"), "left")
        .withColumn("Target", coalesce('Target, lit(0)))
        .withColumn("AdGroupIdEncoded", encodeStringIdUdf('AdGroupId))
        .withColumn("CampaignIdEncoded", encodeStringIdUdf('CampaignId))
        .withColumn("AdvertiserIdEncoded", encodeStringIdUdf('AdvertiserId))
        .withColumn("CPACountWeight", coalesce('CPACountWeight, lit(0)))
        .withColumn("Revenue", coalesce('Revenue, lit(0)).cast("float"))
        .withColumn("PosCount", sum(when($"Target" === lit(1), 1).otherwise(0)).over(win))
        .withColumn("NegCount", sum(when($"Target" === lit(0), 1).otherwise(0)).over(win))
        .withColumn("PosRatio", lit(maxPositiveCount)/$"PosCount")
        .withColumn("NegRatio", when($"PosCount" < lit(maxPositiveCount), $"PosCount").otherwise(lit(maxPositiveCount)) * lit(desiredNegOverPos) / $"NegCount")
        .withColumn("ConversionTrackerLogEntryTime", coalesce('ConversionTrackerLogEntryTime, lit(null).cast("timestamp")))
        .withColumn("UserDataOptIn", lit(2))

      impWithAttr
        .withColumn("rand", rand(seed = samplingSeed))
        .filter("(Target = 1 and rand <= PosRatio) or (Target = 0 and rand < NegRatio)")
        .withColumn("Weight", lit(1))
        .drop("PosCount","NegCount","NegRatio","PosRatio", "rand")
    }).reduce(_.union(_)).cache()

    val preFeatureSamples = sampledImpressionWithAttribution.selectAs[PreFeatureJoinRecordV2]
    val positiveSamples = preFeatureSamples.filter($"Target" === lit(1))
    val negativeSamples = preFeatureSamples.filter($"Target" ===lit(0))

    //adjust weight for positive samples
    val positiveSampleWithAdjustedWeight = 
      if (applyPositiveReweight) {
        positiveSamples.withColumn("Weight", col("CPACountWeight")).selectAs[PreFeatureJoinRecordV2]
      } else {
        positiveSamples
      }

    val negWeightParams = NegativeWeightDistParams(negativeWeightCoefficient, negativeWeightOffset, negativeWeightThreshold)

    val negativeSampleWithAdjustedWeight = 
      if (applyNegativeReweight) {
        generateWeightForNegativeV2[PreFeatureJoinRecordV2](negativeSamples, positiveSamples, conversionLookback, Some(negativeWeightMethod), methodDistParams = negWeightParams)
      } else {
        negativeSamples
      }

    val preFeatureSamplesWithAdjustedWeight = 
        positiveSampleWithAdjustedWeight
        .union(negativeSampleWithAdjustedWeight)
        .groupBy("BidRequestIdStr", "Target").agg(avg("Weight").cast("float").as("Weight"))

    val featureSamplesWithAdjustedWeight = 
      sampledImpressionWithAttribution.drop($"Weight")
      .join(preFeatureSamplesWithAdjustedWeight.select($"BidRequestIdStr",$"Target",$"Weight"), Seq("BidRequestIdStr","Target"), "inner")
      .withColumn("Weight", $"Weight".cast("float"))
      .withColumn("Target", $"Target".cast("float"))

    featureSamplesWithAdjustedWeight.selectAs[UserDataValidationDataForModelTrainingRecord](nullIfAbsent = true)
  }

  override def runTransform(args: Array[String]): Array[(String, Long)] = {
    val trainDataWithFeature = generateTrainSet()(getPrometheus)

    val trainDataWithSplit = trainDataWithFeature
      .withColumn("IsInTrainSet", when(abs(hash($"BidRequestIdStr") % 100) <= trainRatio * 100, lit(true)).otherwise(false))
      .withColumn("split", when($"IsInTrainSet" === lit(true), "train").otherwise("val"))
      .cache()

    val adjustedTrainParquet = 
      if (applyPosNegBalance) { 
        balanceWeightForTrainset(trainDataWithSplit.filter($"split" === lit("train"))
        .selectAs[UserDataValidationDataForModelTrainingRecord],desiredNegOverPos)
      }
      else {
        trainDataWithSplit.filter($"split" === lit("train"))
      }

    val adjustedValParquet = trainDataWithSplit.filter($"split" === lit("val"))

    trainDataWithSplit.unpersist()

    var trainsetRows = Array.fill(2)("", 0L)

    if (saveTrainingDataAsCSV) {
      //save copy with user data
      val parquetSelectionTabular = trainDataWithFeature.columns.map { c => col(c) }.toArray ++ aliasedModelFeatureCols(seqDirectFields ++ seqHashFields)
      var tfDropColumnNames = if (addBidRequestId) {
        rawModelFeatureNames(seqDirectFields)
      } else {
        aliasedModelFeatureNames(keptFields) ++ rawModelFeatureNames(seqDirectFields)
      }

      val userDataCsvDS = if (incTrain) UserDataIncCsvForModelTrainingDatasetLastTouch() else UserDataCsvForModelTrainingDatasetLastTouch()
      val csvTrainRows = userDataCsvDS.writePartition(
        adjustedTrainParquet.select(parquetSelectionTabular: _*).drop(tfDropColumnNames: _*).selectAs[UserDataForModelTrainingRecord](nullIfAbsent = true),
        date, "train", Some(trainSetPartitionCount))
      val csvValRows = userDataCsvDS.writePartition(
        adjustedValParquet.select(parquetSelectionTabular: _*).drop(tfDropColumnNames: _*).selectAs[UserDataForModelTrainingRecord](nullIfAbsent = true),
        date, "val", Some(valSetPartitionCount))
      trainsetRows = Array(csvTrainRows, csvValRows)

      val csvDS = if (incTrain) DataIncCsvForModelTrainingDatasetLastTouch() else DataCsvForModelTrainingDatasetLastTouch()
      csvDS.writePartition(
        adjustedTrainParquet.select(parquetSelectionTabular: _*).drop(tfDropColumnNames: _*).selectAs[DataForModelTrainingRecord](nullIfAbsent = true),
        date, "train", Some(trainSetPartitionCount))
      csvDS.writePartition(
        adjustedValParquet.select(parquetSelectionTabular: _*).drop(tfDropColumnNames: _*).selectAs[DataForModelTrainingRecord](nullIfAbsent = true),
        date, "val", Some(valSetPartitionCount))
    }

    if (saveTrainingDataAsCBuffer) {
      // save as csv with userdata
      val userDataCbufferDS = if (incTrain) ArrayUserDataIncCsvForModelTrainingDatasetLastTouch() else ArrayUserDataCsvForModelTrainingDatasetLastTouch()
      val cbufferTrainRows = userDataCbufferDS.writePartition(encodeDatasetForCBuffer[ArrayUserDataForModelTrainingRecord](adjustedTrainParquet), date, Some("train"), trainSetPartitionCount, trainingBatchSize)
      val cbufferValRows = userDataCbufferDS.writePartition(encodeDatasetForCBuffer[ArrayUserDataForModelTrainingRecord](adjustedValParquet), date, Some("val"), valSetPartitionCount, evalBatchSize)
      trainsetRows = Array(cbufferTrainRows, cbufferValRows)
    }
    trainsetRows
  }
}
