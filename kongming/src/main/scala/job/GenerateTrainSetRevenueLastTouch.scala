package job

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
  val conversionLookback = config.getInt("conversionLookback", 15)

  val saveParquetData = config.getBoolean("saveParquetData", false)
  val saveTrainingDataAsTFRecord = config.getBoolean("saveTrainingDataAsTFRecord", false)
  val saveTrainingDataAsCSV = config.getBoolean("saveTrainingDataAsCSV", true)
  val addBidRequestId = config.getBoolean("addBidRequestId", false)

  val trainSetPartitionCount = config.getInt("trainSetPartitionCount", partCount.trainSet)
  val valSetPartitionCount = config.getInt("valSetPartitionCount", partCount.valSet)


  def generateTrainSetRevenue()(implicit prometheus: PrometheusClient): Dataset[ValidationDataForModelTrainingRecord] = {

    val startDate = date.minusDays(conversionLookback)
    val win = Window.partitionBy($"CampaignIdStr", $"AdGroupIdStr")

    // Imp [T-ConvLB, T]
    val sampledImpressions = (0 to conversionLookback).map(i => {
      val ImpDate = startDate.plusDays(i)
      // Attr [T-ConvLB, T]
      val AttrDates = (ImpDate.toEpochDay to date.toEpochDay).map(LocalDate.ofEpochDay)
      val dailyImp = OldDailyOfflineScoringDataset().readDate(ImpDate)
      val attr = AttrDates.map(dt => {
        DailyAttributionEventsDataset().readPartition(dt, ImpDate.format(DefaultTimeFormatStrings.dateTimeFormatter))
      }).reduce(_.union(_))

      val impWithAttr = dailyImp.join(broadcast(attr.select($"BidRequestId".alias("BidRequestIdStr"), $"Target", $"Revenue")), Seq("BidRequestIdStr"), "left")
        .withColumn("Target", coalesce('Target, lit(0)))
        .withColumn("Revenue", coalesce('Revenue, lit(0)))
        //      .join(scoringSet, Seq("EventDate", "BidRequestIdStr"), "inner")
        .withColumn("PosCount", sum(when($"Target" === lit(1), 1).otherwise(0)).over(win))
        .withColumn("NegCount", sum(when($"Target" === lit(0), 1).otherwise(0)).over(win))
        .withColumn("NegRatio", $"PosCount" * lit(desiredNegOverPos) / $"NegCount")

      impWithAttr.filter($"Target" === lit(1)).union(
          impWithAttr.filter($"Target" ===lit(0))
          .withColumn("rand", rand(seed = samplingSeed))
          .filter($"rand" < $"NegRatio").drop("rand")
        ).withColumn("Weight", lit(1))
        .drop("PosCount","NegCount","NegRatio")

    }).reduce(_.union(_)).cache()

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
      .withColumn("Revenue", least($"Revenue", $"RevenueCap", $"RevenueCapMAD").cast("decimal(10,2)"))
      .select(cols: _*)
      .union(
        sampledImpressions.filter($"Revenue" <= lit(1)).select(cols: _*)
      )

    val parquetSelectionTabular = cappedImpressions.columns.map { c => col(c) }.toArray ++ aliasedModelFeatureCols(seqFields)

    cappedImpressions
      .select(parquetSelectionTabular: _*)
      .selectAs[ValidationDataForModelTrainingRecord]

  }

  override def runTransform(args: Array[String]): Array[(String, Long)] = {
    val trainDataWithFeature = generateTrainSetRevenue()(getPrometheus)
      .withColumn("IsInTrainSet", when(abs(hash($"BidRequestIdStr") % 100) <= trainRatio * 100, lit(true)).otherwise(false))
      .withColumn("split", when($"IsInTrainSet" === lit(true), "train").otherwise("val"))

    val adjustedTrainParquet = trainDataWithFeature.filter($"split" === lit("train"))
    val adjustedValParquet = trainDataWithFeature.filter($"split" === lit("val"))

    var trainsetRows = Array.fill(2)("", 0L)
    // 7. save as parquet and tfrecord
    if (saveParquetData) {
      val parquetTrainRows = ValidationDataForModelTrainingDataset().writePartition(adjustedTrainParquet.selectAs[ValidationDataForModelTrainingRecord], date, "train", Some(trainSetPartitionCount))
      val parquetValRows = ValidationDataForModelTrainingDataset().writePartition(adjustedValParquet.selectAs[ValidationDataForModelTrainingRecord], date, "val", Some(valSetPartitionCount))
      trainsetRows = Array(parquetTrainRows, parquetValRows)
    }

    var tfDropColumnNames = if (addBidRequestId) {
      rawModelFeatureNames(seqFields)
    } else {
      aliasedModelFeatureNames(keptFields) ++ rawModelFeatureNames(seqFields)
    }

    if (saveTrainingDataAsTFRecord) {
      val tfDS = if (incTrain) DataIncForModelTrainingDataset() else DataForModelTrainingDataset()
      val tfTrainRows = tfDS.writePartition(
        adjustedTrainParquet.drop(tfDropColumnNames: _*).selectAs[DataForModelTrainingRecord](nullIfAbsent = true),
        date, "train", Some(trainSetPartitionCount))
      val tfValRows = tfDS.writePartition(
        adjustedValParquet.drop(tfDropColumnNames: _*).selectAs[DataForModelTrainingRecord](nullIfAbsent = true),
        date, "val", Some(valSetPartitionCount))

      trainsetRows = Array(tfTrainRows, tfValRows)
    }

    if (saveTrainingDataAsCSV) {
      val csvDS = DataCsvForModelTrainingDatasetLastTouch()
      val csvTrainRows = csvDS.writePartition(
        adjustedTrainParquet.drop(tfDropColumnNames: _*).selectAs[DataForModelTrainingRecord](nullIfAbsent = true),
        date, "train", Some(trainSetPartitionCount))
      val csvValRows = csvDS.writePartition(
        adjustedValParquet.drop(tfDropColumnNames: _*).selectAs[DataForModelTrainingRecord](nullIfAbsent = true),
        date, "val", Some(valSetPartitionCount))
      trainsetRows = Array(csvTrainRows, csvValRows)
    }

    trainsetRows
  }
}