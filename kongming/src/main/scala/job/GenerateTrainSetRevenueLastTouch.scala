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

import java.time.LocalDate

object GenerateTrainSetRevenueLastTouch extends KongmingBaseJob {

  override def jobName: String = "GenerateTrainSetRevenueLastTouch"

  val incTrain = config.getBoolean("incTrain", false)
  val trainRatio = config.getDouble("trainRatio", 0.8)
  val desiredNegOverPos = config.getInt(path = "desiredPosOverNeg", 9)
  val maxPositiveCount = config.getInt(path="maxPositiveCount", 250000)
  val conversionLookback = config.getInt("conversionLookback", 15)

  val saveTrainingDataAsCSV = config.getBoolean("saveTrainingDataAsCSV", true)
  val saveTrainingDataAsCBuffer = config.getBoolean("saveTrainingDataAsCBuffer", true)
  val addBidRequestId = config.getBoolean("addBidRequestId", false)

  val trainSetPartitionCount = config.getInt("trainSetPartitionCount", partCount.trainSet)
  val valSetPartitionCount = config.getInt("valSetPartitionCount", partCount.valSet)
  val maxConvPerBR = config.getInt("maxConvPerBR", 5)

  def generateTrainSetRevenue()(implicit prometheus: PrometheusClient): Dataset[UserDataValidationDataForModelTrainingRecord] = {
    val startDate = date.minusDays(conversionLookback)
    val win = Window.partitionBy($"CampaignIdEncoded", $"AdGroupIdEncoded")

    // Imp [T-ConvLB, T]
    val sampledImpressions = (0 to conversionLookback).map(i => {
      val ImpDate = startDate.plusDays(i)
      // Attr [T-ConvLB, T]
      val AttrDates = (ImpDate.toEpochDay to date.toEpochDay).map(LocalDate.ofEpochDay)
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
      val attr = AttrDates.map(dt => {
        if (DailyAttributionEventsDataset().partitionExists(dt, ImpDate.format(DefaultTimeFormatStrings.dateTimeFormatter)))
          DailyAttributionEventsDataset().readPartition(dt, ImpDate.format(DefaultTimeFormatStrings.dateTimeFormatter)) else null
      }).filter(_ != null).reduce(_.union(_))

      val winBR = Window.partitionBy("BidRequestId")
      val sampledAttr = attr.withColumn("BRConv", count($"Target").over(winBR))
        .withColumn("ratio", lit(maxConvPerBR) / $"BRConv")
        .withColumn("rand", rand(seed = samplingSeed))
        .filter($"rand" <= $"ratio")
        .drop("BRConv", "ratio", "rand")
        .cache()

      val impWithAttr = dailyImp.join(broadcast(sampledAttr.select($"BidRequestId".alias("BidRequestIdStr"), $"Target", $"Revenue")), Seq("BidRequestIdStr"), "left")
        .withColumn("Target", coalesce('Target, lit(0)))
        .withColumn("Revenue", coalesce('Revenue, lit(0)).cast("float"))
        //      .join(scoringSet, Seq("EventDate", "BidRequestIdStr"), "inner")
        .withColumn("AdGroupIdEncoded", encodeStringIdUdf('AdGroupId))
        .withColumn("CampaignIdEncoded", encodeStringIdUdf('CampaignId))
        .withColumn("AdvertiserIdEncoded", encodeStringIdUdf('AdvertiserId))
        .withColumn("PosCount", sum(when($"Target" === lit(1), 1).otherwise(0)).over(win))
        .withColumn("NegCount", sum(when($"Target" === lit(0), 1).otherwise(0)).over(win))
        .withColumn("NegRatio", $"PosCount" * lit(desiredNegOverPos) / $"NegCount")
        .withColumn("UserDataOptIn", lit(2))

      sampledAttr.unpersist()

      impWithAttr.filter($"Target" === lit(1)).union(
          impWithAttr.filter($"Target" ===lit(0))
          .withColumn("rand", rand(seed = samplingSeed))
          .filter($"rand" < $"NegRatio").drop("rand")
        ).withColumn("Weight", lit(1))
        .drop("PosCount","NegCount","NegRatio")

    }).reduce(_.union(_))
      .withColumn("PosCount", sum(when($"Target" === lit(1), 1).otherwise(0)).over(win))
      .withColumn("NegCount", sum(when($"Target" === lit(0), 1).otherwise(0)).over(win))
      .withColumn("SampleRatio",
        when($"Target" === lit(1), lit(maxPositiveCount) / $"PosCount") // pos sampling ratio
          .otherwise(when($"PosCount" < lit(maxPositiveCount), $"PosCount").otherwise(lit(maxPositiveCount))
            * lit(desiredNegOverPos) / $"NegCount")
      )
      .withColumn("rand", rand(seed = samplingSeed))
      .filter($"rand" < $"SampleRatio").drop("rand")
      .drop("PosCount","NegCount","SampleRatio","rand")
      .cache()

    val cols = sampledImpressions.columns.map(c=>col(c))
    val cappedImpressions = sampledImpressions.filter($"Revenue" > lit(1))
      .withColumn("LogRevenue", log(col("Revenue")))
      // https://www.statisticshowto.com/median-absolute-deviation/
      .withColumn("MedLogRevenue", percentile_approx($"LogRevenue", lit(0.5), lit(100)).over(win))
      .withColumn("MAD", percentile_approx(abs($"LogRevenue" - $"MedLogRevenue"), lit(0.5), lit(100)
      ).over(win))
      .withColumn("RevenueCapMAD", exp(col("MedLogRevenue") + lit(3) * col("MAD")))
      .withColumn("MeanLogRevenue", mean($"LogRevenue").over(win))
      .withColumn("StdLogRevenue", stddev($"LogRevenue").over(win))
      .withColumn("RevenueCap", exp(col("MeanLogRevenue") + lit(3) * col("StdLogRevenue")))
      .withColumn("Revenue", least($"Revenue", $"RevenueCap", $"RevenueCapMAD").cast("float"))
      .withColumn("Target", $"Target".cast("float"))
      .select(cols: _*)
      .union(
        sampledImpressions.filter($"Revenue" <= lit(1)).select(cols: _*)
      )

    cappedImpressions.selectAs[UserDataValidationDataForModelTrainingRecord]
  }

  override def runTransform(args: Array[String]): Array[(String, Long)] = {
    val trainDataWithFeature = generateTrainSetRevenue()(getPrometheus)

    val trainDataWithSplit = trainDataWithFeature
      .repartition(trainSetPartitionCount, rand(seed = samplingSeed)).orderBy(rand(seed = samplingSeed))
      .withColumn("IsInTrainSet", when(abs(hash($"BidRequestIdStr") % 100) <= trainRatio * 100, lit(true)).otherwise(false))
      .withColumn("split", when($"IsInTrainSet" === lit(true), "train").otherwise("val"))

    val adjustedTrainParquet = trainDataWithSplit.filter($"split" === lit("train"))
    val adjustedValParquet = trainDataWithSplit.filter($"split" === lit("val"))

    var trainsetRows = Array.fill(2)("", 0L)

    if (saveTrainingDataAsCSV) {
      // save with user data
      val parquetSelectionTabular = trainDataWithFeature.columns.map { c => col(c) }.toArray ++ aliasedModelFeatureCols(seqDirectFields ++ seqHashFields)
      var tfDropColumnNames = if (addBidRequestId) {
        rawModelFeatureNames(seqDirectFields)
      } else {
        aliasedModelFeatureNames(keptFields) ++ rawModelFeatureNames(seqDirectFields)
      }

      val userDataCsvDS = UserDataCsvForModelTrainingDatasetLastTouch()
      val csvTrainRows = userDataCsvDS.writePartition(
        adjustedTrainParquet.select(parquetSelectionTabular: _*).drop(tfDropColumnNames: _*).selectAs[UserDataForModelTrainingRecord](nullIfAbsent = true),
        date, "train", Some(trainSetPartitionCount))
      val csvValRows = userDataCsvDS.writePartition(
        adjustedValParquet.select(parquetSelectionTabular: _*).drop(tfDropColumnNames: _*).selectAs[UserDataForModelTrainingRecord](nullIfAbsent = true),
        date, "val", Some(valSetPartitionCount))
      trainsetRows = Array(csvTrainRows, csvValRows)

      //save without userdata
      val csvDS = DataCsvForModelTrainingDatasetLastTouch()
      csvDS.writePartition(
        adjustedTrainParquet.select(parquetSelectionTabular: _*).drop(tfDropColumnNames: _*).selectAs[DataForModelTrainingRecord](nullIfAbsent = true),
        date, "train", Some(trainSetPartitionCount))
      csvDS.writePartition(
        adjustedValParquet.select(parquetSelectionTabular: _*).drop(tfDropColumnNames: _*).selectAs[DataForModelTrainingRecord](nullIfAbsent = true),
        date, "val", Some(valSetPartitionCount))
      trainsetRows = Array(csvTrainRows, csvValRows)
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
