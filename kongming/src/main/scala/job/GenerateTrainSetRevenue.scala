package job

import com.thetradedesk.geronimo.shared.{STRING_FEATURE_TYPE, intModelFeaturesCols}
import com.thetradedesk.kongming.features.Features._
import com.thetradedesk.kongming._
import com.thetradedesk.kongming.datasets._
import com.thetradedesk.kongming.transform.ContextualTransform
import com.thetradedesk.kongming.transform.ContextualTransform.ContextualData
import com.thetradedesk.kongming.transform.NegativeTransform.aggregateNegatives
import com.thetradedesk.kongming.transform.TrainSetTransformation._
import com.thetradedesk.spark.TTDSparkContext.spark
import com.thetradedesk.spark.TTDSparkContext.spark.implicits._
import com.thetradedesk.spark.sql.SQLFunctions._
import com.thetradedesk.spark.util.TTDConfig.config
import com.thetradedesk.spark.datasets.core.DefaultTimeFormatStrings
import com.thetradedesk.spark.util.prometheus.PrometheusClient
import org.apache.spark.sql.Dataset
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._
import org.apache.spark.storage.StorageLevel

import java.time.format.DateTimeFormatter

import java.time.LocalDate

/*
  Generate train set for conversion model training job
  Input:
  Daily Negative
  Positive Set
  AdGroup Conversion Tracker Weights

  Output:
  Tensors, seperate train and validation

  Upsampling methods:  1. replicate samples 2. SMOTE
     SMOTE: https://github.com/mjuez/approx-smote


 */

object GenerateTrainSetRevenue extends KongmingBaseJob {

  override def jobName: String = "GenerateTrainSetRevenue"

  val incTrain = config.getBoolean("incTrain", false)
  val trainRatio = config.getDouble("trainRatio", 0.8)
  val desiredNegOverPos = config.getInt(path="desiredPosOverNeg", 9)
  val maxPositiveCount = config.getInt(path="maxPositiveCount", 500000)
  val maxNegativeCount = config.getInt(path="maxNegativeCount", 500000)
  val balanceMethod = config.getString(path="balanceMethod", "downsampling")
  val sampleValSet = config.getBoolean(path = "sampleValSet", true)
  val conversionLookback = config.getInt("conversionLookback", 15)

  val saveParquetData = config.getBoolean("saveParquetData", false)
  val saveTrainingDataAsTFRecord = config.getBoolean("saveTrainingDataAsTFRecord", false)
  val saveTrainingDataAsCSV = config.getBoolean("saveTrainingDataAsCSV", true)
  val addBidRequestId = config.getBoolean("addBidRequestId", false)

  val trainSetPartitionCount = config.getInt("trainSetPartitionCount", partCount.trainSet)
  val valSetPartitionCount = config.getInt("valSetPartitionCount", partCount.valSet)

  val trainsetBalancedCount = config.getInt("trainsetBalancedCount", partCount.TrainsetBalanced)
  val dailyTrainSetWithFeatureCount = config.getInt("dailyTrainSetWithFeatureCount", partCount.DailyTrainsetWithFeature)
  def balancedData(
                    date: LocalDate,
                    adGroupPolicy: Dataset[AdGroupPolicyRecord],
                    adGroupPolicyMapping: Dataset[AdGroupPolicyMappingRecord],
                    maxLookback: Int,
                    rate: Dataset[DailyExchangeRateRecord]  
                  ): Dataset[PreFeatureJoinRecord] = {
    // load aggregated positives
    val aggregatedPositiveSetHist = DailyPositiveBidRequestDataset().readRange(date.minusDays(conversionLookback-1), date, true)
    val aggregatedPositiveSet = (if (incTrain) DailyPositiveBidRequestDataset().readDate(date) else aggregatedPositiveSetHist)
      .withColumn("CurrencyCodeId", $"MonetaryValueCurrency").join(broadcast(rate), Seq("CurrencyCodeId"), "left")
      .withColumn("ValidRevenue", $"MonetaryValue".isNotNull && $"MonetaryValueCurrency".isNotNull)
      .withColumn("FromUSD", when($"MonetaryValueCurrency"==="USD", lit(1)).otherwise($"FromUSD"))
      .withColumn("RevenueInUSD", when($"ValidRevenue", $"MonetaryValue" / $"FromUSD").otherwise(0))
      .withColumn("Revenue", greatest($"RevenueInUSD", lit(1)))
      .withColumn("Weight", lit(1))
      .withColumn("IsInTrainSet", when(abs(hash($"UIID")%100)<=trainRatio*100, lit(true)).otherwise(false))
      .join(broadcast(adGroupPolicy.select("ConfigKey", "ConfigValue")), Seq("ConfigKey", "ConfigValue"), "left_semi")
    
    // Don't bother with config items with no positives in the trainset.
    val preFilteredPolicy = adGroupPolicy
      .join(aggregatedPositiveSet.filter('IsInTrainSet).select("ConfigKey", "ConfigValue").distinct, Seq("ConfigKey", "ConfigValue"), "left_semi")
      .selectAs[AdGroupPolicyRecord]
      .cache

    // 0. load aggregated negatives
    val dailyNegativeSampledBids = DailyNegativeSampledBidRequestDataSet().readRange(date.minusDays(maxLookback-1), date, true)
    val aggregatedNegativeSet = aggregateNegatives(date, dailyNegativeSampledBids, adGroupPolicy, adGroupPolicyMapping)(getPrometheus)
      .withColumn("IsInTrainSet", when($"UIID".isNotNull && $"UIID"=!=lit("00000000-0000-0000-0000-000000000000"),
        when(abs(hash($"UIID")%100)<=trainRatio*100, lit(true)).otherwise(false)).otherwise(
        when(abs(hash($"BidRequestId")%100)<=trainRatio*100, lit(true)).otherwise(false)
      ))
      .withColumn("Weight", lit(1))  // placeholder: 1. assign format with positive 2. we might weight for negative in the future, TBD.
      .withColumn("Revenue", lit(0))
    val window = Window.partitionBy($"ConfigKey", $"ConfigValue")
    // cap on the valid positives (where Revenue>1)
    val cappedPositive = aggregatedPositiveSet.filter($"Revenue">lit(1))
      .withColumn("LogRevenue", log(col("Revenue")))
      .withColumn("MeanLogRevenue", mean($"LogRevenue").over(window))
      .withColumn("StdLogRevenue", stddev($"LogRevenue").over(window))
      .withColumn("RevenueCap", exp(col("MeanLogRevenue") + lit(3) * col("StdLogRevenue")))
      .withColumn("Revenue", least($"Revenue", $"RevenueCap").cast("decimal(10,2)"))
      .drop("LogRevenue", "MeanLogRevenue", "StdLogRevenue", "RevenueCap")
    val aggregatedPositiveSetPreprocessed = cappedPositive.union(
      aggregatedPositiveSet.filter($"Revenue"<=lit(1)))

    // 1. exclude positives from negative; balance neg:pos; remain pos and neg that have both train and val
    val negativeExcludePos = aggregatedNegativeSet.join(
      broadcast(aggregatedPositiveSet.select("ConfigKey", "ConfigValue").distinct), Seq("ConfigKey", "ConfigValue"), "left_semi")
      .join(aggregatedPositiveSetHist, Seq("ConfigValue", "ConfigKey", "BidRequestId"), "left_anti")


    val balancedTrainset = balancePosNeg(
      aggregatedPositiveSetPreprocessed.selectAs[TrainSetRecordVerbose],
      negativeExcludePos.selectAs[TrainSetRecord],
      desiredNegOverPos,
      maxPositiveCount,
      maxNegativeCount,
      balanceMethod = Some(balanceMethod),
      sampleValSet = sampleValSet,
      samplingSeed = samplingSeed)(getPrometheus)

    val balancedPositives = balancedTrainset._1
    val balancedNegatives = balancedTrainset._2

    /*
     Make sure there are train and val in negative samples.
     Make sure there are train in positive samples.
     */
    val dataHaveBothTrainVal = balancedNegatives
      .groupBy("ConfigValue", "ConfigKey").agg(collect_set("IsInTrainSet").as("hasValOrTrain"))
      .filter(size($"hasValOrTrain") === lit(2))
      .join(
        balancedPositives.groupBy("ConfigValue", "ConfigKey").agg(max("IsInTrainSet").as("hasTrain"))
          .filter($"hasTrain" === lit(true)),
        Seq("ConfigValue", "ConfigKey"),
        "inner"
      ).cache()

    val validPositives = balancedPositives.join(dataHaveBothTrainVal, Seq("ConfigValue", "ConfigKey"), "left_semi").selectAs[TrainSetRecordVerbose].cache()
    val validNegatives = balancedNegatives.join(dataHaveBothTrainVal, Seq("ConfigValue", "ConfigKey"), "left_semi").selectAs[TrainSetRecord].cache()

    val preFeatureJoinTrainSet = validPositives
          .withColumn("Target", lit(1))
          .selectAs[PreFeatureJoinRecord]
          .union(validNegatives.withColumn("Target", lit(0)).selectAs[PreFeatureJoinRecord])
    
    preFeatureJoinTrainSet
    
    }

  def generateData(
                  date: LocalDate,
                  bidDate: LocalDate
  ): Dataset[ValidationDataForModelTrainingRecord] = {
    //read the balanced dataset of a given date
    val preFeatureJoinTrainSet = IntermediateTrainDataBalancedDataset()
      .readPartition(date, "biddate", bidDate.format(DefaultTimeFormatStrings.dateTimeFormatter)).as[PreFeatureJoinRecord]
    
    // 5. join all these dataset with bidimpression to get features
    val splitColumn = modelTargetCols(Array(ModelTarget("split", STRING_FEATURE_TYPE, false)))

    // features to hash, including everyone except seq
    var hashFeatures = modelDimensions ++ modelFeatures ++ modelWeights
    hashFeatures = hashFeatures.filter(x => !seqDirectFields.contains(x))
    val tensorflowSelectionTabular = intModelFeaturesCols(hashFeatures) ++ aliasedModelFeatureCols(seqDirectFields) ++ modelTargetCols(modelTargets) ++ splitColumn
    val parquetSelectionTabular = aliasedModelFeatureCols(keptFields ++ directFields) ++ tensorflowSelectionTabular

    val trainDataWithFeature = attachTrainsetWithFeature(preFeatureJoinTrainSet, bidDate, 0)(getPrometheus)
      .withColumn("split", when($"IsInTrainSet", "train").when($"IsTracked" === lit(1), "val").otherwise("untracked"))
      .select(parquetSelectionTabular: _*)
      // TODO: temp set ImpressionPlacementId to empty string
      .withColumn("ImpressionPlacementId",lit(""))
      .as[ValidationDataForModelTrainingRecord]
      .persist(StorageLevel.DISK_ONLY)
    trainDataWithFeature
  }
  override def runTransform(args: Array[String]): Array[(String, Long)] = {
    // test only adgroups in the policy table. since aggKey are all adgroupId, we filter by adgroup id
    val adGroupPolicy = AdGroupPolicyDataset().readDate(date).cache()
    val adGroupPolicyMapping = AdGroupPolicyMappingDataset().readDate(date).cache()

    val rate = DailyExchangeRateDataset().readDate(date).cache()

    // maximum lookback from adgroup's policy
    val maxLookback = adGroupPolicy.agg(max("DataLookBack")).first.getInt(0)

    // Step A: save the balancedData by date and bidDate. Biddate has to be string type to make the write work.
    val trainDataBalanced = balancedData(date, adGroupPolicy, adGroupPolicyMapping, maxLookback, rate)
      .withColumn("biddate", date_format(col("LogEntryTime"), "yyyyMMdd")).selectAs[TrainDataBalancedRecord]

    IntermediateTrainDataBalancedDataset().writePartition(trainDataBalanced, date, "biddate" ,Some(trainsetBalancedCount))
    
    var trainsetRows = Array.fill(3)("", 0L)
    var tfDropColumnNames = if (addBidRequestId) {
        rawModelFeatureNames(seqDirectFields)
      } else {
        aliasedModelFeatureNames(keptFields) ++ rawModelFeatureNames(seqDirectFields)
      }
    // Step B: join feature by bidDate
    (0 to maxLookback-1).par.foreach(
      lookBackDay => {
        val bidDate = date.minusDays(lookBackDay)
        val trainDataWithFeature = generateData(date, bidDate)

        val adjustedTrainParquet = trainDataWithFeature.filter($"split"===lit("train"))
        val adjustedValParquet = trainDataWithFeature.filter($"split"===lit("val"))
        val adjustedUntrackedParquet = trainDataWithFeature.filter($"split"===lit("untracked"))


        //7. save as parquet and tfrecord
        if (saveParquetData) {
          val parquetTrainRows = ROASTemporaryIntermediateValidationDataForModelTrainingDataset(split = "train").writePartition(adjustedTrainParquet, date, bidDate, Some(dailyTrainSetWithFeatureCount))
          val parquetValRows = ROASTemporaryIntermediateValidationDataForModelTrainingDataset(split = "val").writePartition(adjustedValParquet, date, bidDate, Some(dailyTrainSetWithFeatureCount))
          val parquetUntrackedRows = ROASTemporaryIntermediateValidationDataForModelTrainingDataset(split = "untracked").writePartition(adjustedUntrackedParquet, date, bidDate, Some(dailyTrainSetWithFeatureCount))
        }
        // save relevant columns by date and biddate
        ROASTemporaryIntermediateTrainDataWithFeatureDataset(split="train").writePartition(adjustedTrainParquet.drop(tfDropColumnNames: _*)
          .selectAs[DataForModelTrainingRecord](nullIfAbsent = true), date, bidDate, Some(dailyTrainSetWithFeatureCount))

        ROASTemporaryIntermediateTrainDataWithFeatureDataset(split="val").writePartition(adjustedValParquet.drop(tfDropColumnNames: _*)
          .selectAs[DataForModelTrainingRecord](nullIfAbsent = true), date, bidDate, Some(dailyTrainSetWithFeatureCount))

        ROASTemporaryIntermediateTrainDataWithFeatureDataset(split="untracked").writePartition(adjustedUntrackedParquet.drop(tfDropColumnNames: _*)
          .selectAs[DataForModelTrainingRecord](nullIfAbsent = true), date, bidDate, Some(dailyTrainSetWithFeatureCount))
      }
    )

    // Step C
    // load train & val parquet of all biddates
    val traindataWithFeatureAllBidDate = ROASTemporaryIntermediateTrainDataWithFeatureDataset(split = "train").readPartition(date)
    val valdataWithFeatureAllBidDate = ROASTemporaryIntermediateTrainDataWithFeatureDataset(split = "val").readPartition(date)
    val untrackeddataWithFeatureAllBidDate = ROASTemporaryIntermediateTrainDataWithFeatureDataset(split = "untracked").readPartition(date)

    if (saveTrainingDataAsTFRecord) {
      val tfDS = if (incTrain) DataIncForModelTrainingDataset() else DataForModelTrainingDataset()
      val tfTrainRows = tfDS.writePartition(
        traindataWithFeatureAllBidDate.drop(tfDropColumnNames: _*).selectAs[DataForModelTrainingRecord](nullIfAbsent = true),
        date, "train", Some(trainSetPartitionCount))
      val tfValRows = tfDS.writePartition(
        valdataWithFeatureAllBidDate.drop(tfDropColumnNames: _*).selectAs[DataForModelTrainingRecord](nullIfAbsent = true),
        date, "val", Some(valSetPartitionCount))
      val tfUntrackedRows = tfDS.writePartition(
        untrackeddataWithFeatureAllBidDate.drop(tfDropColumnNames: _*).selectAs[DataForModelTrainingRecord](nullIfAbsent = true),
        date, "untracked", Some(valSetPartitionCount))

      trainsetRows = Array(tfTrainRows, tfValRows, tfUntrackedRows)
    }

    if (saveTrainingDataAsCSV) {
      val csvDS = if (incTrain) DataIncCsvForModelTrainingDataset() else DataCsvForModelTrainingDataset()
      val csvTrainRows = csvDS.writePartition(
        traindataWithFeatureAllBidDate.drop(tfDropColumnNames: _*).selectAs[DataForModelTrainingRecord](nullIfAbsent = true),
        date, "train", Some(trainSetPartitionCount))
      val csvValRows = csvDS.writePartition(
        valdataWithFeatureAllBidDate.drop(tfDropColumnNames: _*).selectAs[DataForModelTrainingRecord](nullIfAbsent = true),
        date, "val", Some(valSetPartitionCount))
      val csvUntrackedRows = csvDS.writePartition(
        untrackeddataWithFeatureAllBidDate.drop(tfDropColumnNames: _*).selectAs[DataForModelTrainingRecord](nullIfAbsent = true),
        date, "untracked", Some(valSetPartitionCount))

      trainsetRows = Array(csvTrainRows, csvValRows, csvUntrackedRows)
    }
    trainsetRows

  }
}
