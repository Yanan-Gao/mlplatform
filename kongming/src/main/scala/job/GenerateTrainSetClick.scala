package job

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
  val addBidRequestId = config.getBoolean("addBidRequestId", false)

  val trainSetPartitionCount = config.getInt("trainSetPartitionCount", partCount.trainSet)
  val valSetPartitionCount = config.getInt("valSetPartitionCount", partCount.valSet)


  def generateTrainSetClick()(implicit prometheus: PrometheusClient): Dataset[UserDataValidationDataForModelTrainingRecord] = {

    val lookback = if (incTrain) 0 else clickLookback

    val startDate = date.minusDays(lookback)
    val win = Window.partitionBy($"CampaignIdStr", $"AdGroupIdStr")

    // Imp [T-ConvLB, T]
    val sampledImpressions = (0 to lookback).map(i => {
      val ImpDate = startDate.plusDays(i)
      // Attr [T-ConvLB, T]
      val dailyImp = OldDailyOfflineScoringDataset().readDate(ImpDate)
      val dailyClick = DailyClickDataset().readDate(ImpDate)
      val impWithAttr = dailyImp.join(
        dailyClick.withColumnRenamed("BidRequestId", "BidRequestIdStr")
        .select("BidRequestIdStr", "ClickRedirectId"), Seq("BidRequestIdStr"), "left")
        .withColumn("Target", when(col("ClickRedirectId").isNotNull, 1).otherwise(0)).cache()
        .withColumn("PosCount", sum(when($"Target" === lit(1), 1).otherwise(0)).over(win))
        .withColumn("NegCount", sum(when($"Target" === lit(0), 1).otherwise(0)).over(win))
        .withColumn("NegRatio", $"PosCount" * lit(desiredNegOverPos) / $"NegCount")

      impWithAttr.filter($"Target" === lit(1)).union(
          impWithAttr.filter($"Target" ===lit(0))
          .withColumn("rand", rand(seed = samplingSeed))
          .filter($"rand" < $"NegRatio").drop("rand")
        ).withColumn("Weight", lit(1))
        .drop("PosCount","NegCount","NegRatio")

    }).reduce(_.union(_))
      .withColumn("UserDataOptIn",lit(2))
      .cache()
    // userdataoptin hashmod 1 ->2

    val parquetSelectionTabular = sampledImpressions.columns.map { c => col(c) }.toArray ++ aliasedModelFeatureCols(seqDirectFields ++ seqHashFields)

    sampledImpressions
      .select(parquetSelectionTabular: _*)
      .selectAs[UserDataValidationDataForModelTrainingRecord](nullIfAbsent = true)

  }

  override def runTransform(args: Array[String]): Array[(String, Long)] = {
    val trainDataWithFeature = generateTrainSetClick()(getPrometheus)
      .withColumn("IsInTrainSet", when(abs(hash($"BidRequestIdStr") % 100) <= trainRatio * 100, lit(true)).otherwise(false))
      .withColumn("split", when($"IsInTrainSet" === lit(true), "train").otherwise("val"))

    val adjustedTrainParquet = trainDataWithFeature.filter($"split" === lit("train"))
    val adjustedValParquet = trainDataWithFeature.filter($"split" === lit("val"))

    var trainsetRows = Array.fill(2)("", 0L)
    var tfDropColumnNames = if (addBidRequestId) {
      rawModelFeatureNames(seqDirectFields)
    } else {
      aliasedModelFeatureNames(keptFields) ++ rawModelFeatureNames(seqDirectFields)
    }

    // save as csv no userdata
    if (saveTrainingDataAsCSV) {
      val csvDS = if (incTrain) DataIncCsvForModelTrainingDatasetClick() else DataCsvForModelTrainingDatasetClick()
      val csvTrainRows = csvDS.writePartition(
        adjustedTrainParquet.drop(aliasedModelFeatureNames(userFeatures):_*).drop(tfDropColumnNames: _*).selectAs[DataForModelTrainingRecord](nullIfAbsent = true),
        date, "train", Some(trainSetPartitionCount))
      val csvValRows = csvDS.writePartition(
        adjustedValParquet.drop(aliasedModelFeatureNames(userFeatures):_*).drop(tfDropColumnNames: _*).selectAs[DataForModelTrainingRecord](nullIfAbsent = true),
        date, "val", Some(valSetPartitionCount))
      trainsetRows = Array(csvTrainRows, csvValRows)
    }

    // save as csv with userdata
    if (saveTrainingDataAsCSV) {
      // with userdata trainset
      val csvDS = if (incTrain) UserDataIncCsvForModelTrainingDatasetClick() else UserDataCsvForModelTrainingDatasetClick()
      val csvTrainRows = csvDS.writePartition(
        adjustedTrainParquet.drop(tfDropColumnNames: _*).selectAs[UserDataForModelTrainingRecord](nullIfAbsent = true),
        date, "train", Some(trainSetPartitionCount))
      val csvValRows = csvDS.writePartition(
        adjustedValParquet.drop(tfDropColumnNames: _*).selectAs[UserDataForModelTrainingRecord](nullIfAbsent = true),
        date, "val", Some(valSetPartitionCount))
      trainsetRows = Array(csvTrainRows, csvValRows)
    }

    trainsetRows
  }
}