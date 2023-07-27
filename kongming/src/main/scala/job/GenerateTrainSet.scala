package job

import org.apache.spark.sql.functions._
import org.apache.spark.sql.Column
import com.thetradedesk.geronimo.shared.{STRING_FEATURE_TYPE, intModelFeaturesCols}
import com.thetradedesk.kongming.features.Features._
import com.thetradedesk.kongming._
import com.thetradedesk.kongming.datasets._
import com.thetradedesk.kongming.transform.NegativeTransform.aggregateNegatives
import com.thetradedesk.kongming.transform.TrainSetTransformation._
import com.thetradedesk.spark.TTDSparkContext.spark
import com.thetradedesk.spark.TTDSparkContext.spark.implicits._
import com.thetradedesk.spark.util.prometheus.PrometheusClient
import com.thetradedesk.spark.util.TTDConfig.config
import com.thetradedesk.spark.sql.SQLFunctions._

import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.DoubleType
import org.apache.spark.storage.StorageLevel

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

object GenerateTrainSet {

  def main(args: Array[String]): Unit = {
    val prometheus = new PrometheusClient(KongmingApplicationName, getJobNameWithExperimentName("GenerateTrainSet"))
    val jobDurationGauge = prometheus.createGauge(RunTimeGaugeName, "Job execution time in seconds")
    val jobDurationGaugeTimer = jobDurationGauge.startTimer()
    val outputRowsWrittenGauge = prometheus.createGauge(OutputRowCountGaugeName, "Number of rows written", "DataSet")

    val incTrain = config.getBoolean("incTrain", false)
    val trainRatio = config.getDouble("trainRatio", 0.8)
    val desiredNegOverPos = config.getInt(path="desiredPosOverNeg", 9)
    val maxPositiveCount = config.getInt(path="maxPositiveCount", 500000)
    val maxNegativeCount = config.getInt(path="maxNegativeCount", 500000)
    val balanceMethod = config.getString(path="balanceMethod", "downsampling")
    val sampleValSet = config.getBoolean(path = "sampleValSet", true)
    val conversionLookback = config.getInt("conversionLookback", 7)

    val negativeWeightMethod = config.getString("negativeWeightMethod", "PostDistVar")

    /*
    Params for func generateWeightForNegative
    The default Offset and Coefficient is derived from f = 1 - a * exp(b*x) that fits the overall distribution of positive samples
    Need to be updated if the distribution change dramatically
     */
    val negativeWeightOffset = config.getDouble("negativeWeightOffset", 0.7)
    val negativeWeightCoefficient = config.getDouble("negativeWeightCoef", -0.33)
    val negativeWeightThreshold = config.getInt("negativeWeightThreshold", 3000)

    val saveParquetData = config.getBoolean("saveParquetData", false)
    val saveTrainingDataAsTFRecord = config.getBoolean("saveTrainingDataAsTFRecord", false)
    val saveTrainingDataAsCSV = config.getBoolean("saveTrainingDataAsCSV", true)
    val addBidRequestId = config.getBoolean("addBidRequestId", false)

    val experimentName = config.getString("trainSetExperimentName" , "")

    val trainSetPartitionCount = config.getInt("trainSetPartitionCount", 1000)
    val valSetPartitionCount = config.getInt("valSetPartitionCount", 1000)

    // test only adgroups in the policy table. since aggKey are all adgroupId, we filter by adgroup id
    val adGroupPolicy = AdGroupPolicyDataset().readDate(date).cache()

    // maximum lookback from adgroup's policy
    val maxLookback = adGroupPolicy.agg(max("DataLookBack")).first.getInt(0)

    // 0. load aggregated negatives
    val dailyNegativeSampledBids = DailyNegativeSampledBidRequestDataSet().readRange(date.minusDays(maxLookback-1), date, true)
    val aggregatedNegativeSet = aggregateNegatives(date, dailyNegativeSampledBids, adGroupPolicy)(prometheus)
      .withColumn("IsInTrainSet", when($"UIID".isNotNull && $"UIID"=!=lit("00000000-0000-0000-0000-000000000000"),
        when(abs(hash($"UIID")%100)<=trainRatio*100, lit(true)).otherwise(false)).otherwise(
        when(abs(hash($"BidRequestId")%100)<=trainRatio*100, lit(true)).otherwise(false)
      ))
      .withColumn("Weight", lit(1))  // placeholder: 1. assign format with positive 2. we might weight for negative in the future, TBD.

    //    load aggregated positives
    val aggregatedPositiveSetHist = DailyPositiveBidRequestDataset().readRange(date.minusDays(conversionLookback-1), date, true)
    val aggregatedPositiveSet = (if (incTrain) DailyPositiveBidRequestDataset().readDate(date) else aggregatedPositiveSetHist)
      .withColumn("IsInTrainSet", when(abs(hash($"UIID")%100)<=trainRatio*100, lit(true)).otherwise(false))

    // 1. exclude positives from negative; balance neg:pos; remain pos and neg that have both train and val
    val negativeExcludePos = aggregatedNegativeSet.join(
      broadcast(aggregatedPositiveSet.select("DataAggValue").distinct), Seq("DataAggValue"), "left_semi")
      .join(aggregatedPositiveSetHist, Seq("ConfigValue", "ConfigKey", "BidRequestId"), "left_anti")

    val balancedTrainset = balancePosNeg(
      aggregatedPositiveSet.withColumn("Weight", lit(1)).selectAs[TrainSetRecordVerbose],
      negativeExcludePos.selectAs[TrainSetRecord],
      desiredNegOverPos,
      maxPositiveCount,
//      if (incTrain) maxPositiveCount/maxLookback else maxPositiveCount,
      maxNegativeCount,
      balanceMethod = Some(balanceMethod),
      sampleValSet = sampleValSet,
      samplingSeed = samplingSeed)(prometheus)

    val balancedPositives = balancedTrainset._1
    val balancedNegatives = balancedTrainset._2

    /*
     Make sure there are train and val in negative samples.
     Make sure there are train in positive samples.
     */
    val  dataHaveBothTrainVal = balancedNegatives
      .groupBy("ConfigValue","ConfigKey").agg(collect_set("IsInTrainSet").as("hasValOrTrain"))
      .filter(size($"hasValOrTrain")===lit(2))
      .join(
        balancedPositives.groupBy("ConfigValue","ConfigKey").agg(max("IsInTrainSet").as("hasTrain"))
          .filter($"hasTrain"===lit(true)),
        Seq("ConfigValue","ConfigKey"),
        "inner"
      ).cache()

    val validPositives = balancedPositives.join(dataHaveBothTrainVal, Seq("ConfigValue","ConfigKey" ), "left_semi" )
    val validNegatives = balancedNegatives.join(dataHaveBothTrainVal, Seq("ConfigValue","ConfigKey"), "left_semi" ).selectAs[TrainSetRecord].cache()


    // 2. get the latest weights for trackingtags for adgroups in policytable
    val trackingTagWindow =  Window.partitionBy($"TrackingTagId", $"ConfigKey", $"ConfigValue")
      .orderBy($"ReportingColumnId")
    val adGroupDS = UnifiedAdGroupDataSet().readLatestPartitionUpTo(date, true)
    val trackingTagWithWeight = getWeightsForTrackingTags(date, adGroupPolicy, adGroupDS, normalized = true)
      .withColumn("ReportingColumnRank", dense_rank().over(trackingTagWindow))
      .filter($"ReportingColumnRank"===lit(1))
      .drop("ReportingColumnId")
    // to simplify, use the weights of the first reportingcolumn

    // 3. transform weights for positive label
    // todo: multi days positive might have different click and  view lookback window, might not alligned with latest weight
    val positivesWithRawWeight = validPositives.join(trackingTagWithWeight, Seq("TrackingTagId", "ConfigValue", "ConfigKey"))
      .withColumn("NormalizedPixelWeight", $"NormalizedPixelWeight".cast(DoubleType))
      .withColumn("NormalizedCustomCPAClickWeight", $"NormalizedCustomCPAClickWeight".cast(DoubleType))
      .withColumn("NormalizedCustomCPAViewthroughWeight", $"NormalizedCustomCPAViewthroughWeight".cast(DoubleType))
      .selectAs[PositiveWithRawWeightsRecord]

    val adjustedPosWithWeight = generateWeightForPositive(positivesWithRawWeight)(prometheus).cache()
    // todo: normalize weight for positives, otherwise pos/neg will be changed if neg has no weight
    // Question: do we need to upsample or just tune weights are enough, consider the naive upsamping. Upsampling can have different strategy though.

    // weighting negatives after balancing to optimize the runtime
    val negWeightParams = NegativeWeightDistParams(negativeWeightCoefficient, negativeWeightOffset, negativeWeightThreshold)
    val adjustedNegWithWeight = generateWeightForNegative(validNegatives, positivesWithRawWeight, maxLookback, Some(negativeWeightMethod), methodDistParams = negWeightParams)(prometheus).cache()

    val adjustedNeg = adjustedNegWithWeight.withColumn("Target", lit(0))
    val adjustedPos = adjustedPosWithWeight.withColumn("Target", lit(1))

    val preFeatureJoinTrainSet = adjustedPos.union(adjustedNeg)
      .selectAs[PreFeatureJoinRecord].cache()

    //adjust weight in trainset
    val adjustedWeightDataset= adjustWeightForTrainset(preFeatureJoinTrainSet,desiredNegOverPos)

    // 5. join all these dataset with bidimpression to get features
    val splitColumn = modelTargetCols(Array(ModelTarget("split", STRING_FEATURE_TYPE, false)))

    // features to hash, including everyone except seq
    var hashFeatures = modelDimensions ++ modelFeatures ++ modelWeights
    hashFeatures = hashFeatures.filter(x => !(seqFields ++ directFields).contains(x))
    val tensorflowSelectionTabular = intModelFeaturesCols(hashFeatures) ++ rawModelFeatureCols(directFields) ++ aliasedModelFeatureCols(seqFields) ++ modelTargetCols(modelTargets) ++ splitColumn
    val parquetSelectionTabular = aliasedModelFeatureCols(keptFields) ++ tensorflowSelectionTabular

    val trainDataWithFeature = attachTrainsetWithFeature(adjustedWeightDataset, maxLookback)(prometheus)
      .withColumn("split", when($"IsInTrainSet"===lit(true), "train").otherwise("val"))
      .select(parquetSelectionTabular: _*)
      .as[ValidationDataForModelTrainingRecord]
      .persist(StorageLevel.DISK_ONLY)

    // 6. split train and val
    val adjustedTrainParquet = trainDataWithFeature.filter($"split"===lit("train"))
    val adjustedValParquet = trainDataWithFeature.filter($"split"===lit("val"))

    // 7. save as parquet and tfrecord
    if (saveParquetData) {
      val parquetTrainRows = ValidationDataForModelTrainingDataset(experimentName).writePartition(adjustedTrainParquet, date, "train", Some(trainSetPartitionCount))
      val parquetValRows = ValidationDataForModelTrainingDataset(experimentName).writePartition(adjustedValParquet, date, "val", Some(valSetPartitionCount))
      outputRowsWrittenGauge.labels("ValidationDataForModelTrainingDataset/ParquetTrain").set(parquetTrainRows)
      outputRowsWrittenGauge.labels("ValidationDataForModelTrainingDataset/ParquetVal").set(parquetValRows)
    }

    var tfDropColumnNames = if (addBidRequestId) {
      rawModelFeatureNames(seqFields)
    } else {
      aliasedModelFeatureNames(keptFields) ++ rawModelFeatureNames(seqFields)
    }

    if (saveTrainingDataAsTFRecord) {
      val tfDS = if (incTrain) DataIncForModelTrainingDataset(experimentName) else DataForModelTrainingDataset(experimentName)
      val tfTrainRows = tfDS.writePartition(
        adjustedTrainParquet.drop(tfDropColumnNames: _*).as[DataForModelTrainingRecord],
        date, "train", Some(trainSetPartitionCount))
      val tfValRows = tfDS.writePartition(
        adjustedValParquet.drop(tfDropColumnNames: _*).as[DataForModelTrainingRecord],
        date, "val", Some(valSetPartitionCount))

      outputRowsWrittenGauge.labels("DataForModelTrainingDataset/TFTrain").set(tfTrainRows)
      outputRowsWrittenGauge.labels("DataForModelTrainingDataset/TFVal").set(tfValRows)
    }

    if (saveTrainingDataAsCSV) {
      val csvDS = if (incTrain) DataIncCsvForModelTrainingDataset(experimentName) else DataCsvForModelTrainingDataset(experimentName)
      val csvTrainRows = csvDS.writePartition(
        adjustedTrainParquet.drop(tfDropColumnNames: _*).as[DataForModelTrainingRecord],
        date, "train", Some(trainSetPartitionCount))
      val csvValRows = csvDS.writePartition(
        adjustedValParquet.drop(tfDropColumnNames: _*).as[DataForModelTrainingRecord],
        date, "val", Some(valSetPartitionCount))

      outputRowsWrittenGauge.labels("DataForModelTrainingDataset/CsvTrain").set(csvTrainRows)
      outputRowsWrittenGauge.labels("DataForModelTrainingDataset/CsvVal").set(csvValRows)
    }

    jobDurationGaugeTimer.setDuration()
    prometheus.pushMetrics()

    spark.stop()

  }
}
