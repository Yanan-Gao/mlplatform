package job

import com.thetradedesk.geronimo.bidsimpression.schema.{BidsImpressions, BidsImpressionsSchema}
import com.thetradedesk.geronimo.shared.schemas.ModelFeature

import java.time.LocalDate
import org.apache.spark.sql.functions._
import org.apache.spark.sql.Column
import com.thetradedesk.kongming.datasets.{AdGroupPolicyDataset, DailyNegativeSampledBidRequestDataSet, DailyNegativeSampledBidRequestRecord, DailyPositiveBidRequestDataset, DailyPositiveLabelRecord, DataForModelTrainingDataset, DataForModelTrainingRecord}
import com.thetradedesk.spark.util.prometheus.PrometheusClient
import com.thetradedesk.kongming.date
import com.thetradedesk.spark.TTDSparkContext.spark
import com.thetradedesk.spark.TTDSparkContext.spark.implicits._
import com.thetradedesk.geronimo.shared.{intModelFeaturesCols, loadParquetData}
import com.thetradedesk.kongming.policyDate
import com.thetradedesk.kongming.transform.NegativeTransform.aggregateNegatives
import com.thetradedesk.spark.util.TTDConfig.config
import com.thetradedesk.spark.sql.SQLFunctions._
import com.thetradedesk.kongming.transform.TrainSetTransformation._
import org.apache.spark.sql.types.DoubleType
import org.apache.spark.storage.StorageLevel
import job.DailyOfflineScoringSet.{keptFields, modelKeepFeatureCols}
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

  val modelFeatures: Array[ModelFeature] = Array(
    ModelFeature("AdGroupId", STRING_FEATURE_TYPE, Some(500002), 0),

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


  def main(args: Array[String]): Unit = {
    val prometheus = new PrometheusClient("KoaV4Conversion", "GenerateTrainSet")
    val trainRatio = config.getDouble("trainRatio", 0.8)
    val desiredNegOverPos = config.getInt(path="desiredPosOverNeg", 9)
    val maxNegativeCount = config.getInt(path="maxNegativeCount", 500000)
    val upSamplingValSet = config.getBoolean(path = "upSamplingValSet", false)
    val conversionLookback = config.getInt("conversionLookback", 7)

    // test only adgroups in the policy table. since aggKey are all adgroupId, we filter by adgroup id
    val adGroupPolicyHardCodedDate = policyDate
    val adGroupPolicy = AdGroupPolicyDataset.readHardCodedDataset(adGroupPolicyHardCodedDate).cache()

    // maximum lookback from adgroup's policy
    val maxLookback = adGroupPolicy.agg(max("DataLookBack")).first.getInt(0)

    // 0. load aggregated negatives
    val dailyNegativeSampledBids = loadParquetData[DailyNegativeSampledBidRequestRecord](DailyNegativeSampledBidRequestDataSet.S3BasePath, date, lookBack = Some(maxLookback))
    val aggregatedNegativeSet = aggregateNegatives(dailyNegativeSampledBids, adGroupPolicy)(prometheus)
      .withColumn("IsInTrainSet", when(abs(hash($"BidRequestId")%100)<=trainRatio*100, lit(true)).otherwise(false))
      .withColumn("Weight", lit(1))  // placeholder: 1. assign format with positive 2. we might weight for negative in the future, TBD.

    //    load aggregated positives
    val aggregatedPositiveSet =  loadParquetData[DailyPositiveLabelRecord](DailyPositiveBidRequestDataset.S3BasePath, date, lookBack = Some(conversionLookback-1) )
      .withColumn("IsInTrainSet", when(abs(hash($"BidRequestId")%100)<=trainRatio*100, lit(true)).otherwise(false))


    // 1. exclude positives from negative;  remain pos and neg that have both train and val
    val negativeExcludePos = aggregatedNegativeSet
      .join(aggregatedPositiveSet, Seq( "ConfigKey", "ConfigValue", "BidRequestId"), joinType = "left_anti")
      .selectAs[TrainSetRecord]
      .cache()


    //   todo: ensure that each ConfigKey/ConfigValue pairs have both train and validation data instead of just filtering them out
    /*
     current thought is: if there are no val set that's probably because positives are too less so the hash mod is biased.
     The model probably is not going to perform anyways with too less positive.
     But it's better to at least have a model.
     */
    val  dataHaveBothTrainVal = negativeExcludePos
      .groupBy("ConfigKey", "ConfigValue").agg(collect_set("IsInTrainSet").as("hasValOrTrain"))
      .filter(size($"hasValOrTrain")===lit(2))
      .join(
        aggregatedPositiveSet.groupBy("ConfigKey", "ConfigValue").agg(collect_set("IsInTrainSet").as("hasValOrTrain"))
          .filter(size($"hasValOrTrain")===lit(2)),
        Seq("ConfigKey", "ConfigValue" ),
        "inner"
      ).cache()

    val validPositives = aggregatedPositiveSet.join(dataHaveBothTrainVal, Seq("ConfigKey", "ConfigValue" ), "left_semi" )
    val validNegatives = negativeExcludePos.join(dataHaveBothTrainVal, Seq("ConfigKey", "ConfigValue" ), "left_semi" ).selectAs[TrainSetRecord].cache()


    // 2. get the latest weights for adgroups in policytable
    val trackingTagWithWeight = getWeightsForTrackingTags(adGroupPolicy)

    // 3. transform weights for positive label
    // todo: multi days positive might have different click and  view lookback window, might not alligned with latest weight
    val positivesWithRawWeight = validPositives.join(trackingTagWithWeight, Seq("TrackingTagId", "ConfigKey", "ConfigValue"))
      .withColumn("NormalizedPixelWeight", $"NormalizedPixelWeight".cast(DoubleType))
      .withColumn("NormalizedCustomCPAClickWeight", $"NormalizedCustomCPAClickWeight".cast(DoubleType))
      .withColumn("NormalizedCustomCPAViewthroughWeight", $"NormalizedCustomCPAViewthroughWeight".cast(DoubleType))
      .selectAs[PositiveWithRawWeightsRecord]

    val realPositives = generateWeightForPositive(positivesWithRawWeight)(prometheus).cache()
    // todo: normalize weight for positives, otherwise pos/neg will be changed if neg has no weight
    // Question: do we need to upsample or just tune weights are enough, consider the naive upsamping. Upsampling can have different strategy though.


    // 4. balance  pos and neg
    val balancedTrainset= balancePosNeg(realPositives, validNegatives, desiredNegOverPos, maxNegativeCount, upSamplingValSet = upSamplingValSet)(prometheus)

    val adjustedPos = balancedTrainset._1.withColumn("Target", lit(1))
    val adjustedNeg = balancedTrainset._2.withColumn("Target", lit(0))

    val preFeatureJoinTrainSet = adjustedPos.union(adjustedNeg)
      .selectAs[PreFeatureJoinRecord].cache()

  // 5. join all these dataset with bidimpression to get features , join by day
    val trainDataWithFeature = attachTrainsetWithFeature(preFeatureJoinTrainSet, maxLookback)(prometheus).persist(StorageLevel.MEMORY_AND_DISK)

    // 6. split train and val
    val adjustedTrain  = trainDataWithFeature.filter($"IsInTrainSet"===lit(true)).cache()
    val adjustedVal = trainDataWithFeature.filter($"IsInTrainSet"===lit(false)).cache()

    // 7. save as tfrecord and parquet
    val selectionTabular = intModelFeaturesCols(modelFeatures ++ modelWeights)  ++ modelTargetCols(modelTargets)

    val dfTuple = Seq(
      (adjustedTrain,"train", "parquet"),
      (adjustedVal, "val", "parquet"),
      (adjustedTrain,"train", "tfrecord"),
      (adjustedVal, "val", "tfrecord")
    )

    dfTuple.foreach{
      case (df, df_split, df_format) => {

        val dfTransformed = df_format match {
          case "parquet" => df.select((modelKeepFeatureCols(keptFields) ++ selectionTabular): _*).as[DataForModelTrainingRecord]
          case "tfrecord" => df.select(selectionTabular: _*).as[DataForModelTrainingRecord]
        }

        DataForModelTrainingDataset.writePartition(
          dfTransformed,
          date,
          subFolderKey = Some("split"),
          subFolderValue = Some(df_split.concat("_").concat(df_format)),
          format = Some(df_format)
        )

      }
    }


  }


}
