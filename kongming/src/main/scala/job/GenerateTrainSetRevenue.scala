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
import com.thetradedesk.spark.util.prometheus.PrometheusClient
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._
import org.apache.spark.storage.StorageLevel

import java.time.format.DateTimeFormatter

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
  override def runTransform(args: Array[String]): Array[(String, Long)] = {

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

    // test only adgroups in the policy table. since aggKey are all adgroupId, we filter by adgroup id
    val adGroupPolicy = AdGroupPolicyDataset().readDate(date).cache()
    val adGroupPolicyMapping = AdGroupPolicyMappingDataset().readDate(date).cache()

    val rate = DailyExchangeRateDataset().readDate(date).cache()

    // maximum lookback from adgroup's policy
    val maxLookback = adGroupPolicy.agg(max("DataLookBack")).first.getInt(0)

    //    load aggregated positives
    val aggregatedPositiveSetHist = DailyPositiveBidRequestDataset().readRange(date.minusDays(conversionLookback-1), date, true)
    val aggregatedPositiveSet = (if (incTrain) DailyPositiveBidRequestDataset().readDate(date) else aggregatedPositiveSetHist)
      .withColumn("CurrencyCodeId", $"MonetaryValueCurrency").join(broadcast(rate), Seq("CurrencyCodeId"), "left")
      .withColumn("ValidRevenue", $"MonetaryValue".isNotNull && $"MonetaryValueCurrency".isNotNull)
      .withColumn("FromUSD", when($"MonetaryValueCurrency"==="USD", lit(1)).otherwise($"FromUSD"))
      .withColumn("RevenueInUSD", when($"ValidRevenue", $"MonetaryValue" / $"FromUSD").otherwise(0))
      .withColumn("Revenue", greatest($"RevenueInUSD", lit(1)))
      .withColumn("Weight", lit(1))
      .withColumn("IsInTrainSet", when(abs(hash($"UIID")%100)<=trainRatio*100, lit(true)).otherwise(false))
      .drop("ConfigValue", "ConfigKey")
      .join(broadcast(adGroupPolicy.select("ConfigValue", "ConfigKey", "DataAggKey", "DataAggValue")), Seq("DataAggKey","DataAggValue"), "inner")

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

    val window = Window.partitionBy($"DataAggValue")
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
      broadcast(aggregatedPositiveSet.select("DataAggValue").distinct), Seq("DataAggValue"), "left_semi")
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

    // 5. join all these dataset with bidimpression to get features
    val splitColumn = modelTargetCols(Array(ModelTarget("split", STRING_FEATURE_TYPE, false)))

    // features to hash, including everyone except seq
    var hashFeatures = modelDimensions ++ modelFeatures ++ modelWeights
    hashFeatures = hashFeatures.filter(x => !seqFields.contains(x))
    val tensorflowSelectionTabular = intModelFeaturesCols(hashFeatures) ++ aliasedModelFeatureCols(seqFields) ++ modelTargetCols(modelTargets) ++ splitColumn
    val parquetSelectionTabular = aliasedModelFeatureCols(keptFields) ++ tensorflowSelectionTabular

    val trainDataWithFeature = attachTrainsetWithFeature(preFeatureJoinTrainSet, maxLookback)(getPrometheus)
      .withColumn("split", when($"IsInTrainSet"===lit(true), "train").otherwise("val"))
      .select(parquetSelectionTabular: _*)
      // TODO: temp set ImpressionPlacementId to empty string
      .withColumn("ImpressionPlacementId",lit(""))
      .as[ValidationDataForModelTrainingRecord]
      .persist(StorageLevel.DISK_ONLY)

    // 6. split train and val
    val adjustedTrainParquet = trainDataWithFeature.filter($"split"===lit("train"))
    val adjustedValParquet = trainDataWithFeature.filter($"split"===lit("val"))

    var trainsetRows = Array.fill(2)("", 0L)
    // 7. save as parquet and tfrecord
    if (saveParquetData) {
      val parquetTrainRows = ValidationDataForModelTrainingDataset().writePartition(adjustedTrainParquet, date, "train", Some(trainSetPartitionCount))
      val parquetValRows = ValidationDataForModelTrainingDataset().writePartition(adjustedValParquet, date, "val", Some(valSetPartitionCount))
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
      val csvDS = if (incTrain) DataIncCsvForModelTrainingDataset() else DataCsvForModelTrainingDataset()
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
