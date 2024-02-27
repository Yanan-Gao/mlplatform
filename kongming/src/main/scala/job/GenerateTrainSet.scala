package job

import com.thetradedesk.geronimo.shared.{STRING_FEATURE_TYPE, intModelFeaturesCols}
import com.thetradedesk.kongming._
import com.thetradedesk.kongming.datasets._
import com.thetradedesk.kongming.features.Features.{ModelTarget, aliasedModelFeatureCols, aliasedModelFeatureNames, keptFields, modelDimensions, modelFeatures, modelTargetCols, modelTargets, modelWeights, rawModelFeatureNames, seqFields}
import com.thetradedesk.kongming.transform.NegativeTransform.aggregateNegatives
import com.thetradedesk.kongming.transform.TrainSetTransformation._
import com.thetradedesk.spark.TTDSparkContext.spark.implicits._
import com.thetradedesk.spark.datasets.core.DefaultTimeFormatStrings
import com.thetradedesk.spark.sql.SQLFunctions._
import com.thetradedesk.spark.util.TTDConfig.config
import org.apache.spark.sql.Dataset
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.DoubleType
import org.apache.spark.storage.StorageLevel

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

object GenerateTrainSet extends KongmingBaseJob {

  override def jobName: String = "GenerateTrainSet"

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
                    maxLookback: Int
                  ): Dataset[PreFeatureJoinRecord] = {
    // 0. load aggregated negatives
    val aggregatedPositiveSetHist = DailyPositiveBidRequestDataset().readRange(date.minusDays(conversionLookback-1), date, true)
    val aggregatedPositiveSet = (if (incTrain) DailyPositiveBidRequestDataset().readDate(date) else aggregatedPositiveSetHist)
      .withColumn("IsInTrainSet", abs(hash($"UIID") % 100) <= trainRatio * 100)
      .join(broadcast(adGroupPolicy.select("ConfigKey", "ConfigValue")), Seq("ConfigKey", "ConfigValue"), "left_semi")

    val dailyNegativeSampledBids = DailyNegativeSampledBidRequestDataSet().readRange(date.minusDays(maxLookback-1), date, true)
      .selectAs[DailyNegativeSampledBidRequestRecord]

    // Don't bother with config items with no positives in the trainset.
    val preFilteredPolicy = adGroupPolicy
      .join(aggregatedPositiveSet.filter('IsInTrainSet).select("ConfigKey", "ConfigValue").distinct, Seq("ConfigKey", "ConfigValue"), "left_semi")
      .selectAs[AdGroupPolicyRecord]

    val aggregatedNegativeSet = aggregateNegatives(date, dailyNegativeSampledBids, preFilteredPolicy, adGroupPolicyMapping)(getPrometheus)
      .withColumn("IsInTrainSet", when($"UIID".isNotNull && $"UIID"=!=lit("00000000-0000-0000-0000-000000000000"),
        when(abs(hash($"UIID")%100)<=trainRatio*100, lit(true)).otherwise(false)).otherwise(
        when(abs(hash($"BidRequestId")%100)<=trainRatio*100, lit(true)).otherwise(false)
      ))
      .withColumn("Weight", lit(1))  // placeholder: 1. assign format with positive 2. we might weight for negative in the future, TBD.

    // 1. exclude positives from negative; balance neg:pos; remain pos and neg that have both train and val
    val negativeExcludePos = aggregatedNegativeSet.join(broadcast(aggregatedPositiveSet.select("ConfigKey", "ConfigValue").distinct), Seq("ConfigKey", "ConfigValue"), "left_semi")
      .join(aggregatedPositiveSetHist, Seq("ConfigValue", "ConfigKey", "BidRequestId"), "left_anti")

    val balancedTrainset = balancePosNeg(
      // placeholder for Revenue, otherwise set nullIfAbsent=true may accidentally silent other absence
      aggregatedPositiveSet.withColumn("Weight", lit(1)).withColumn("Revenue", lit(0)).selectAs[TrainSetRecordVerbose],
      negativeExcludePos.withColumn("Revenue", lit(0)).selectAs[TrainSetRecord],
      desiredNegOverPos,
      maxPositiveCount,
      //      if (incTrain) maxPositiveCount/maxLookback else maxPositiveCount,
      maxNegativeCount,
      balanceMethod = Some(balanceMethod),
      sampleValSet = sampleValSet,
      samplingSeed = samplingSeed)(getPrometheus)

    val balancedPositives = balancedTrainset._1.persist(StorageLevel.DISK_ONLY)
    val balancedNegatives = balancedTrainset._2.persist(StorageLevel.DISK_ONLY)

    /*
     Make sure there are train and val in negative samples.
     Make sure there are train in positive samples.
     */
    val dataHaveBothTrainVal = balancedNegatives
      .groupBy("ConfigValue","ConfigKey").agg(collect_set("IsInTrainSet").as("hasValOrTrain"))
      .filter(size($"hasValOrTrain")===lit(2))
      .join(
        balancedPositives.groupBy("ConfigValue","ConfigKey").agg(max("IsInTrainSet").as("hasTrain"))
          .filter($"hasTrain"===lit(true)),
        Seq("ConfigValue","ConfigKey"),
        "inner"
      ).cache()

    val validPositives = balancedPositives.join(dataHaveBothTrainVal, Seq("ConfigValue","ConfigKey"), "left_semi" )
    val validNegatives = balancedNegatives.join(dataHaveBothTrainVal, Seq("ConfigValue","ConfigKey"), "left_semi" ).selectAs[TrainSetRecord]

    // 2. get the latest weights for trackingtags for adgroups in policytable
    val trackingTagWindow =  Window.partitionBy($"TrackingTagId", $"ConfigKey", $"ConfigValue")
      .orderBy($"ReportingColumnId")
    val trackingTagWithWeight = getWeightsForTrackingTags(date, adGroupPolicyMapping, normalized = true)
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

    val adjustedPosWithWeight = generateWeightForPositive(positivesWithRawWeight)(getPrometheus)
    // todo: normalize weight for positives, otherwise pos/neg will be changed if neg has no weight
    // Question: do we need to upsample or just tune weights are enough, consider the naive upsamping. Upsampling can have different strategy though.

    // weighting negatives after balancing to optimize the runtime
    val negWeightParams = NegativeWeightDistParams(negativeWeightCoefficient, negativeWeightOffset, negativeWeightThreshold)
    val adjustedNegWithWeight = generateWeightForNegative(validNegatives, positivesWithRawWeight, maxLookback, Some(negativeWeightMethod), methodDistParams = negWeightParams)(getPrometheus)

    val adjustedNeg = adjustedNegWithWeight.withColumn("Target", lit(0))
    val adjustedPos = adjustedPosWithWeight.withColumn("Target", lit(1))

    val preFeatureJoinTrainSet = adjustedPos.union(adjustedNeg)
      .selectAs[PreFeatureJoinRecord]

    //adjust weight in trainset
    val adjustedWeightDataset = adjustWeightForTrainset(preFeatureJoinTrainSet,desiredNegOverPos)

    adjustedWeightDataset
  }

  def generateData(date: LocalDate, bidDate: LocalDate): Dataset[ValidationDataForModelTrainingRecord] = {
    // Read the balanced data set of given biddate.
    val adjustedWeightDataset =  IntermediateTrainDataBalancedDataset()
      .readPartition(date, "biddate", bidDate.format(DefaultTimeFormatStrings.dateTimeFormatter)).as[PreFeatureJoinRecord]

    // 6. split train and val
    val splitColumn = modelTargetCols(Array(ModelTarget("split", STRING_FEATURE_TYPE, false)))

    // features to hash, including everyone except seq
    var hashFeatures = modelDimensions ++ modelFeatures ++ modelWeights
    hashFeatures = hashFeatures.filter(x => !(seqFields).contains(x))
    val tensorflowSelectionTabular = intModelFeaturesCols(hashFeatures) ++ aliasedModelFeatureCols(seqFields) ++ modelTargetCols(modelTargets) ++ splitColumn
    //    hashFeatures = hashFeatures.filter(x => !(seqFields ++ directFields).contains(x))
    //    val tensorflowSelectionTabular = intModelFeaturesCols(hashFeatures) ++ rawModelFeatureCols(directFields) ++ aliasedModelFeatureCols(seqFields) ++ modelTargetCols(modelTargets) ++ splitColumn

    val parquetSelectionTabular = aliasedModelFeatureCols(keptFields) ++ tensorflowSelectionTabular

    // split train and val
    val trainDataWithFeature = attachTrainsetWithFeature(adjustedWeightDataset, bidDate, 0)(getPrometheus)
      .withColumn("split", when($"IsInTrainSet"===lit(true), "train").otherwise("val"))
      .select(parquetSelectionTabular: _*)
      .selectAs[ValidationDataForModelTrainingRecord](nullIfAbsent=true)
    trainDataWithFeature
  }

  override def runTransform(args: Array[String]): Array[(String, Long)] = {
    // test only adgroups in the policy table. since aggKey are all adgroupId, we filter by adgroup id
    val adGroupPolicy = AdGroupPolicyDataset().readDate(date)
    val adGroupPolicyMapping = AdGroupPolicyMappingDataset().readDate(date)
    // maximum lookback from adgroup's policy
    val maxLookback = adGroupPolicy.agg(max("DataLookBack")).first.getInt(0)

    // Step A: save the balancedData by date and bidDate. Biddate has to be string type to make the write work.
    val trainDataBalanced = balancedData(date, adGroupPolicy, adGroupPolicyMapping, maxLookback)
      .withColumn("biddate", date_format(col("LogEntryTime"), "yyyyMMdd")).selectAs[TrainDataBalancedRecord]

    IntermediateTrainDataBalancedDataset().writePartition(trainDataBalanced, date, "biddate" ,Some(trainsetBalancedCount))

    // Step B: join feature by bidDate
    (0 to maxLookback).par.foreach(
      lookBackDay => {
        val bidDate = date.minusDays(lookBackDay)
        val trainDataWithFeature = generateData(date, bidDate)

        val adjustedTrainParquet = trainDataWithFeature.filter($"split"===lit("train"))
        val adjustedValParquet = trainDataWithFeature.filter($"split"===lit("val"))

        // save raw parquet when needed
        if (saveParquetData){
          val parquetTrainRows = IntermediateValidationDataForModelTrainingDataset(split = "train").writePartition(adjustedTrainParquet, date, bidDate, Some(dailyTrainSetWithFeatureCount))
          val parquetValRows = IntermediateValidationDataForModelTrainingDataset(split = "val").writePartition(adjustedValParquet, date, bidDate, Some(dailyTrainSetWithFeatureCount))

        }

        var tfDropColumnNames = if (addBidRequestId) {
          rawModelFeatureNames(seqFields)
        } else {
          aliasedModelFeatureNames(keptFields) ++ rawModelFeatureNames(seqFields)
        }

        // save relevant columns by date and biddate
        IntermediateTrainDataWithFeatureDataset(split="train").writePartition(adjustedTrainParquet.drop(tfDropColumnNames: _*)
          .selectAs[DataForModelTrainingRecord](nullIfAbsent = true), date, bidDate, Some(dailyTrainSetWithFeatureCount))

        IntermediateTrainDataWithFeatureDataset(split="val").writePartition(adjustedValParquet.drop(tfDropColumnNames: _*)
          .selectAs[DataForModelTrainingRecord](nullIfAbsent = true), date, bidDate, Some(dailyTrainSetWithFeatureCount))

      }
    )

    // Step C
    // load train & val parquet of all biddates
    val traindataWithFeatureAllBidDate = IntermediateTrainDataWithFeatureDataset(split = "train").readPartition(date)
    val valdataWithFeatureAllBidDate = IntermediateTrainDataWithFeatureDataset(split = "val").readPartition(date)

    var trainsetRows = Array.fill(2)("", 0L)

    // save as csv
    if (saveTrainingDataAsCSV) {
      val csvDS = if (incTrain) DataIncCsvForModelTrainingDataset() else DataCsvForModelTrainingDataset()
      val csvTrainRows = csvDS.writePartition(
        traindataWithFeatureAllBidDate.orderBy(rand()).drop("v","split","date","biddate").selectAs[DataForModelTrainingRecord](nullIfAbsent = true),
        date, "train", Some(trainSetPartitionCount))
      val csvValRows = csvDS.writePartition(
        valdataWithFeatureAllBidDate.orderBy(rand()).drop("v","split","date","biddate").selectAs[DataForModelTrainingRecord](nullIfAbsent = true),
        date, "val", Some(valSetPartitionCount))
      trainsetRows = Array(csvTrainRows, csvValRows)
    }
    trainsetRows
  }

}