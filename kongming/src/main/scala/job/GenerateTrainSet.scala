package job

import com.thetradedesk.geronimo.shared.schemas.ModelFeature
import org.apache.spark.sql.functions._
import org.apache.spark.sql.Column
import com.thetradedesk.kongming._
import com.thetradedesk.kongming.datasets._
import com.thetradedesk.spark.util.prometheus.PrometheusClient
import com.thetradedesk.spark.TTDSparkContext.spark
import com.thetradedesk.spark.TTDSparkContext.spark.implicits._
import com.thetradedesk.geronimo.shared.{ARRAY_INT_FEATURE_TYPE, intModelFeaturesCols}
import com.thetradedesk.kongming.policyDate
import com.thetradedesk.kongming.transform.NegativeTransform.aggregateNegatives
import com.thetradedesk.spark.util.TTDConfig.config
import com.thetradedesk.spark.sql.SQLFunctions._
import com.thetradedesk.kongming.transform.TrainSetTransformation._
import org.apache.spark.sql.types.DoubleType
import job.DailyOfflineScoringSet.{keptFields, modelKeepFeatureColNames}
import org.apache.spark.sql.expressions.Window
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

  val STRING_FEATURE_TYPE = "string"
  val INT_FEATURE_TYPE = "int"
  val FLOAT_FEATURE_TYPE = "float"

  val modelWeights: Array[ModelFeature] = Array(ModelFeature("Weight", FLOAT_FEATURE_TYPE, None, 0))

  val modelDimensions: Array[ModelFeature] = Array(ModelFeature("AdGroupId", STRING_FEATURE_TYPE, Some(500002), 0))
  val modelFeatures: Array[ModelFeature] = Array(

    ModelFeature("SupplyVendor", STRING_FEATURE_TYPE, Some(102), 0),
    ModelFeature("SupplyVendorPublisherId", STRING_FEATURE_TYPE, Some(200002), 0),
    ModelFeature("Site", STRING_FEATURE_TYPE, Some(500002), 0),
    ModelFeature("AdFormat", STRING_FEATURE_TYPE, Some(202), 0),

    ModelFeature("Country", STRING_FEATURE_TYPE, Some(252), 0),
    ModelFeature("Region", STRING_FEATURE_TYPE, Some(4002), 0),
    ModelFeature("City", STRING_FEATURE_TYPE, Some(150002), 0),
    ModelFeature("Zip", STRING_FEATURE_TYPE, Some(90002), 0),
    ModelFeature("DeviceMake", STRING_FEATURE_TYPE, Some(6002), 0),
    ModelFeature("DeviceModel", STRING_FEATURE_TYPE, Some(40002), 0),
    ModelFeature("RequestLanguages", STRING_FEATURE_TYPE, Some(5002), 0),


    // these are already integers
    ModelFeature("RenderingContext", INT_FEATURE_TYPE, Some(6), 0),

    ModelFeature("DeviceType", INT_FEATURE_TYPE, Some(9), 0),
    ModelFeature("OperatingSystem", INT_FEATURE_TYPE, Some(72), 0),
    ModelFeature("Browser", INT_FEATURE_TYPE, Some(15), 0),
    ModelFeature("InternetConnectionType", INT_FEATURE_TYPE, Some(10), 0),
    ModelFeature("MatchedFoldPosition", INT_FEATURE_TYPE, Some(5), 0),

    // made its card=3 to avoid all 1's after hashing
    ModelFeature("HasContextualCategoryTier1", INT_FEATURE_TYPE, Some(3), 0),
    ModelFeature("ContextualCategoryLengthTier1", FLOAT_FEATURE_TYPE, None, 0),
    ModelFeature("ContextualCategoriesTier1", ARRAY_INT_FEATURE_TYPE, Some(31), 0),

    ModelFeature("sin_hour_day", FLOAT_FEATURE_TYPE, None, 0),
    ModelFeature("cos_hour_day", FLOAT_FEATURE_TYPE, None, 0),
    ModelFeature("sin_minute_hour", FLOAT_FEATURE_TYPE, None, 0),
    ModelFeature("cos_minute_hour", FLOAT_FEATURE_TYPE, None, 0),
    ModelFeature("sin_hour_week", FLOAT_FEATURE_TYPE, None, 0),
    ModelFeature("cos_hour_week", FLOAT_FEATURE_TYPE, None, 0),
    ModelFeature("latitude", FLOAT_FEATURE_TYPE, None, 0),
    ModelFeature("longitude", FLOAT_FEATURE_TYPE, None, 0)

  )
  case class ModelTarget(name: String, dtype: String, nullable: Boolean)

  val modelTargets = Vector(
    ModelTarget("Target", "Float", nullable = false)
  )

  def modelTargetCols(targets: Seq[ModelTarget]): Array[Column] = {
    targets.map(t => col(t.name).alias(t.name)).toArray
  }

  val seqFields: Array[ModelFeature] = Array(
    ModelFeature("ContextualCategoriesTier1", ARRAY_INT_FEATURE_TYPE, Some(31), 0),
  )

  def seqModelFeaturesCols(inputColAndDims: Seq[ModelFeature]): Array[Column] = {
    inputColAndDims.map {
      case ModelFeature(name, ARRAY_INT_FEATURE_TYPE, Some(cardinality), _) =>
        (0 until cardinality).map(c => when(col(name).isNotNull && size(col(name))>c, col(name)(c)).otherwise(0).alias(name+s"_Column$c"))
    }.toArray.flatMap(_.toList)
  }

  def modelKeepFeatureCols(features: Seq[ModelFeature]): Array[Column] = {
    features.map(f => col(f.name).alias(f.name + "Str")).toArray
  }


  def main(args: Array[String]): Unit = {
    val prometheus = new PrometheusClient(KongmingApplicationName, "GenerateTrainSet")
    val jobDurationGauge = prometheus.createGauge(RunTimeGaugeName, "Job execution time in seconds")
    val jobDurationGaugeTimer = jobDurationGauge.startTimer()
    val outputRowsWrittenGauge = prometheus.createGauge(OutputRowCountGaugeName, "Number of rows written", "DataSet")

    val trainRatio = config.getDouble("trainRatio", 0.8)
    val desiredNegOverPos = config.getInt(path="desiredPosOverNeg", 9)
    val maxNegativeCount = config.getInt(path="maxNegativeCount", 500000)
    val upSamplingValSet = config.getBoolean(path = "upSamplingValSet", false)
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
    val saveCSV = config.getBoolean("saveCSV", true)

    val experimentName = config.getString("trainSetExperimentName" , "")

    // test only adgroups in the policy table. since aggKey are all adgroupId, we filter by adgroup id
    val adGroupPolicy = AdGroupPolicySnapshotDataset().readDataset(date).cache()

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
    val aggregatedPositiveSet =  DailyPositiveBidRequestDataset().readRange(date.minusDays(conversionLookback-1), date, true)
      .withColumn("IsInTrainSet", when(abs(hash($"UIID")%100)<=trainRatio*100, lit(true)).otherwise(false))

    // 1. exclude positives from negative;  remain pos and neg that have both train and val
    val negativeExcludePos = aggregatedNegativeSet
      .join(aggregatedPositiveSet, Seq("ConfigValue","ConfigKey", "BidRequestId"), joinType = "left_anti")
      .selectAs[TrainSetRecord]
      .cache()

    /*
     Make sure there are train and val in negative samples.
     Make sure there are train in positive samples.
     */
    val  dataHaveBothTrainVal = negativeExcludePos
      .groupBy("ConfigValue","ConfigKey").agg(collect_set("IsInTrainSet").as("hasValOrTrain"))
      .filter(size($"hasValOrTrain")===lit(2))
      .join(
        aggregatedPositiveSet.groupBy("ConfigValue","ConfigKey").agg(max("IsInTrainSet").as("hasTrain"))
          .filter($"hasTrain"===lit(true)),
        Seq("ConfigValue","ConfigKey"),
        "inner"
      ).cache()

    val validPositives = aggregatedPositiveSet.join(dataHaveBothTrainVal, Seq("ConfigValue","ConfigKey" ), "left_semi" )
    val validNegatives = negativeExcludePos.join(dataHaveBothTrainVal, Seq("ConfigValue","ConfigKey"), "left_semi" ).selectAs[TrainSetRecord].cache()


    // 2. get the latest weights for trackingtags for adgroups in policytable
    val trackingTagWindow =  Window.partitionBy($"TrackingTagId", $"ConfigKey", $"ConfigValue")
      .orderBy($"ReportingColumnId")
    val adGroupDS = UnifiedAdGroupDataSet().readLatestPartitionUpTo(date)
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

    val realPositives = generateWeightForPositive(positivesWithRawWeight)(prometheus).cache()
    // todo: normalize weight for positives, otherwise pos/neg will be changed if neg has no weight
    // Question: do we need to upsample or just tune weights are enough, consider the naive upsamping. Upsampling can have different strategy though.

    // 4. balance  pos and neg
    val balancedTrainset= balancePosNeg(realPositives, validNegatives, desiredNegOverPos, maxNegativeCount, upSamplingValSet = upSamplingValSet, samplingSeed = samplingSeed)(prometheus)

    // weighting negatives after balancing to optimize the runtime
    val negWeightParams = NegativeWeightDistParams(negativeWeightCoefficient, negativeWeightOffset, negativeWeightThreshold)
    val adjustedNegWithWeight = generateWeightForNegative(balancedTrainset._2, positivesWithRawWeight, maxLookback, Some(negativeWeightMethod), methodDistParams = negWeightParams)(prometheus).cache()

    val adjustedNeg = adjustedNegWithWeight.withColumn("Target", lit(0))
    val adjustedPos = balancedTrainset._1.withColumn("Target", lit(1))

    val preFeatureJoinTrainSet = adjustedPos.union(adjustedNeg)
      .selectAs[PreFeatureJoinRecord].cache()

    //adjust weight in trainset
    val adjustedWeightDataset= adjustWeightForTrainset(preFeatureJoinTrainSet,desiredNegOverPos)

    // 5. join all these dataset with bidimpression to get features
    val splitColumn = modelTargetCols(Array(ModelTarget("split", STRING_FEATURE_TYPE, false)))
    // features to hash, including everyone except seq
    var hashFeatures = modelDimensions ++ modelFeatures ++ modelWeights
    hashFeatures = hashFeatures.filter(x => !seqFields.contains(x))
    val tensorflowSelectionTabular = intModelFeaturesCols(hashFeatures) ++ seqModelFeaturesCols(seqFields) ++ modelTargetCols(modelTargets) ++ splitColumn
    val parquetSelectionTabular = modelKeepFeatureCols(keptFields) ++ tensorflowSelectionTabular

    val trainDataWithFeature = attachTrainsetWithFeature(adjustedWeightDataset, maxLookback)(prometheus)
      .withColumn("split",
        when($"IsInTrainSet"===lit(true), "train").otherwise("val"))
      .select(parquetSelectionTabular: _*)
      .as[ValidationDataForModelTrainingRecord]
      .persist(StorageLevel.DISK_ONLY)

    // 6. split train and val
    val adjustedTrainParquet  = trainDataWithFeature.filter($"split"===lit("train"))
    val adjustedValParquet = trainDataWithFeature.filter($"split"===lit("val"))

    // 7. save as parquet and tfrecord
    if (saveParquetData) {
      val parquetTrainRows = ValidationDataForModelTrainingDataset(experimentName).writePartition(adjustedTrainParquet, date, "train", Some(100))
      val parquetValRows = ValidationDataForModelTrainingDataset(experimentName).writePartition(adjustedValParquet, date, "val", Some(100))
      outputRowsWrittenGauge.labels("ValidationDataForModelTrainingDataset/ParquetTrain").set(parquetTrainRows)
      outputRowsWrittenGauge.labels("ValidationDataForModelTrainingDataset/ParquetVal").set(parquetValRows)
    }

    val tfDropColumnNames = modelKeepFeatureColNames(keptFields)

    if (saveTrainingDataAsTFRecord) {
      val tfTrainRows = DataForModelTrainingDataset(experimentName).writePartition(
        adjustedTrainParquet.drop(tfDropColumnNames: _*).as[DataForModelTrainingRecord],
        date, "train", Some(100))
      val tfValRows = DataForModelTrainingDataset(experimentName).writePartition(
        adjustedValParquet.drop(tfDropColumnNames: _*).as[DataForModelTrainingRecord],
        date, "val", Some(100))

      outputRowsWrittenGauge.labels("DataForModelTrainingDataset/TFTrain").set(tfTrainRows)
      outputRowsWrittenGauge.labels("DataForModelTrainingDataset/TFVal").set(tfValRows)
    }

    if (saveCSV) {
      val csvTrainRows = DataCsvForModelTrainingDataset(experimentName).writePartition(
        adjustedTrainParquet.drop(tfDropColumnNames: _*)
//          .drop($"ContextualCategories")
          .drop($"ContextualCategoriesTier1")
          .as[DataForModelTrainingRecord],
        date, "train", Some(200))
      val csvValRows = DataCsvForModelTrainingDataset(experimentName).writePartition(
        adjustedValParquet.drop(tfDropColumnNames: _*)
//          .drop($"ContextualCategories")
          .drop($"ContextualCategoriesTier1")
          .as[DataForModelTrainingRecord],
        date, "val", Some(100))

      outputRowsWrittenGauge.labels("DataForModelTrainingDataset/CsvTrain").set(csvTrainRows)
      outputRowsWrittenGauge.labels("DataForModelTrainingDataset/CsvVal").set(csvValRows)
    }

    // 8. save the adgroupIdInt for base adgroups(configvalue) and associated adgroups
    val adgroupBaseAssociateMapping = getBaseAssociateAdGroupIntMappings(adGroupPolicy, adGroupDS)
    BaseAssociateAdGroupMappingIntDataset().writePartition(adgroupBaseAssociateMapping, date, Some(1))

    jobDurationGaugeTimer.setDuration()
    prometheus.pushMetrics()

    spark.stop()

  }
}
