package job

import com.thetradedesk.geronimo.shared.encodeStringIdUdf
import com.thetradedesk.kongming._
import com.thetradedesk.kongming.datasets._
import com.thetradedesk.kongming.features.Features._
import com.thetradedesk.spark.TTDSparkContext.spark.implicits._
import com.thetradedesk.spark.sql.SQLFunctions._
import com.thetradedesk.spark.util.TTDConfig.config
import com.thetradedesk.spark.util.prometheus.PrometheusClient
import org.apache.spark.sql.Dataset
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._

import java.time.LocalDate

object GenerateTrainSetClick extends KongmingBaseJob {

  override def jobName: String = "GenerateTrainSetClick"

  val incTrain = config.getBoolean("incTrain", false)
  val trainRatio = config.getDouble("trainRatio", 0.8)
  val desiredNegOverPos = config.getInt(path = "desiredPosOverNeg", 9)
  val clickLookback = config.getInt("clickLookback", 15)

  val saveTrainingDataAsCSV = config.getBoolean("saveTrainingDataAsCSV", true)
  val saveTrainingDataAsCBuffer = config.getBoolean("saveTrainingDataAsCBuffer", true)
  val addBidRequestId = config.getBoolean("addBidRequestId", false)

  val trainSetPartitionCount = config.getInt("trainSetPartitionCount", partCount.trainSet)
  val valSetPartitionCount = config.getInt("valSetPartitionCount", partCount.valSet)

  def generateTrainSetClick()(implicit prometheus: PrometheusClient): Dataset[UserDataValidationDataForModelTrainingRecord] = {

    val lookback = if (incTrain) 0 else clickLookback

    val startDate = date.minusDays(lookback)
    val win = Window.partitionBy($"CampaignIdEncoded", $"AdGroupIdEncoded")

    // Imp [T-ConvLB, T]
    val sampledImpressions = (0 to lookback).map(i => {
      val ImpDate = startDate.plusDays(i)
      // Attr [T-ConvLB, T]
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

      val dailyClick = DailyClickDataset().readDate(ImpDate)
      val impWithAttr = dailyImp.join(
        dailyClick.withColumnRenamed("BidRequestId", "BidRequestIdStr")
        .select("BidRequestIdStr", "ClickRedirectId"), Seq("BidRequestIdStr"), "left")
        .withColumn("Target", when(col("ClickRedirectId").isNotNull, 1).otherwise(0)).cache()
        .withColumn("AdGroupIdEncoded", encodeStringIdUdf('AdGroupId))
        .withColumn("CampaignIdEncoded", encodeStringIdUdf('CampaignId))
        .withColumn("AdvertiserIdEncoded", encodeStringIdUdf('AdvertiserId))
        .withColumn("PosCount", sum(when($"Target" === lit(1), 1).otherwise(0)).over(win))
        .withColumn("NegCount", sum(when($"Target" === lit(0), 1).otherwise(0)).over(win))
        .withColumn("NegRatio", $"PosCount" * lit(desiredNegOverPos) / $"NegCount")
        .withColumn("Revenue", lit(0))

      impWithAttr.filter($"Target" === lit(1)).union(
          impWithAttr.filter($"Target" ===lit(0))
          .withColumn("rand", rand(seed = samplingSeed))
          .filter($"rand" < $"NegRatio").drop("rand")
        ).withColumn("Weight", lit(1))
        .drop("PosCount","NegCount","NegRatio")
    }).reduce(_.union(_))
      .withColumn("UserDataOptIn",lit(2))
      .withColumn("Target", $"Target".cast("float"))
      .cache()
    // userdataoptin hashmod 1 ->2

    sampledImpressions.selectAs[UserDataValidationDataForModelTrainingRecord](nullIfAbsent = true)
  }

  override def runTransform(args: Array[String]): Array[(String, Long)] = {
    val trainDataWithFeature = generateTrainSetClick()(getPrometheus)

    val trainDataWithSplit = trainDataWithFeature
      .withColumn("IsInTrainSet", when(abs(hash($"BidRequestIdStr") % 100) <= trainRatio * 100, lit(true)).otherwise(false))
      .withColumn("split", when($"IsInTrainSet" === lit(true), "train").otherwise("val"))

    val adjustedTrainParquet = trainDataWithSplit.filter($"split" === lit("train"))
    val adjustedValParquet = trainDataWithSplit.filter($"split" === lit("val"))

    var trainsetRows = Array.fill(2)("", 0L)
    if (saveTrainingDataAsCSV) {
      // save as csv no userdata
      val parquetSelectionTabular = trainDataWithFeature.columns.map { c => col(c) }.toArray ++ aliasedModelFeatureCols(seqDirectFields ++ seqHashFields)
      var tfDropColumnNames = if (addBidRequestId) {
        rawModelFeatureNames(seqDirectFields)
      } else {
        aliasedModelFeatureNames(keptFields) ++ rawModelFeatureNames(seqDirectFields)
      }

      val csvDS = if (incTrain) DataIncCsvForModelTrainingDatasetClick() else DataCsvForModelTrainingDatasetClick()
      csvDS.writePartition(
        adjustedTrainParquet.select(parquetSelectionTabular: _*).drop(tfDropColumnNames: _*).selectAs[DataForModelTrainingRecord](nullIfAbsent = true),
        date, "train", Some(trainSetPartitionCount))
      csvDS.writePartition(
        adjustedValParquet.select(parquetSelectionTabular: _*).drop(tfDropColumnNames: _*).selectAs[DataForModelTrainingRecord](nullIfAbsent = true),
        date, "val", Some(valSetPartitionCount))

      // save as csv with userdata
      val userDataCsvDS = if (incTrain) UserDataIncCsvForModelTrainingDatasetClick() else UserDataCsvForModelTrainingDatasetClick()
      val csvTrainRows = userDataCsvDS.writePartition(
        adjustedTrainParquet.select(parquetSelectionTabular: _*).drop(tfDropColumnNames: _*).selectAs[UserDataForModelTrainingRecord](nullIfAbsent = true),
        date, "train", Some(trainSetPartitionCount))
      val csvValRows = userDataCsvDS.writePartition(
        adjustedValParquet.select(parquetSelectionTabular: _*).drop(tfDropColumnNames: _*).selectAs[UserDataForModelTrainingRecord](nullIfAbsent = true),
        date, "val", Some(valSetPartitionCount))
      trainsetRows = Array(csvTrainRows, csvValRows)
    }

    if (saveTrainingDataAsCBuffer) {
      // save as csv with userdata
      val userDataCbufferDS = if (incTrain) ArrayUserDataIncCsvForModelTrainingDatasetClick() else ArrayUserDataCsvForModelTrainingDatasetClick()
      val cbufferTrainRows = userDataCbufferDS.writePartition(encodeDatasetForCBuffer[ArrayUserDataForModelTrainingRecord](adjustedTrainParquet), date, Some("train"), trainSetPartitionCount, trainingBatchSize)
      val cbufferValRows = userDataCbufferDS.writePartition(encodeDatasetForCBuffer[ArrayUserDataForModelTrainingRecord](adjustedValParquet), date, Some("val"), valSetPartitionCount, evalBatchSize)
      trainsetRows = Array(cbufferTrainRows, cbufferValRows)
    }
    trainsetRows
  }
}
