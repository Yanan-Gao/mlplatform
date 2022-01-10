package com.ttd.benchmarks.conversion

import com.github.nscala_time.time.Imports.DateTime
import com.ttd.benchmarks.automl.{ApproximateQuantileSplitter, CustomValidationSplitter, FitOnce, MlFlowExperiment, TrainValidationSplitBenchmark}
import com.ttd.benchmarks.util.logging.{BidFeedback, ClickTracker, DateHourPartitionedDataset}
import org.apache.spark.ml.evaluation.BinaryClassificationEvaluator
import org.apache.spark.ml.linalg.Vector
import org.apache.spark.ml.param.{DoubleParam, Param, ParamMap, ParamValidators}
import org.apache.spark.ml.util.Identifiable
import org.apache.spark.ml.{Estimator, PredictionModel}
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions.{avg, col, lit, unix_timestamp}
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.{DataFrame, Dataset, SparkSession}

/**
 * Enum trait for defining supported ways of splitting a train and test set for the clicktracker benchmark
 */
sealed trait ClickTrackerSplit {
  /**
   * Metadata params that are logged to MlFlow about the benchmark run type
   */
  def logParams: Map[String, String]

  def formatDay(day: DateTime): String = day.toString("yyyyMMdd HH")
}

/**
 * Class that specifies sequences of hours to read for train and test datasets
 */
case class TrainTestDateSplit(bidTrainDates: Seq[DateTime], bidTestDates: Seq[DateTime],
                              clickTrainDates: Seq[DateTime], clickTestDates: Seq[DateTime]) extends ClickTrackerSplit {
  override def logParams: Map[String, String] =
    Map(
      "trainDates" -> bidTrainDates.map(formatDay).toString(),
      "testDates" -> bidTestDates.map(formatDay).toString()
    )
}

/**
 * Class that specifies sequences of hours to read and a quantile for splitting the train and test times
 */
case class QuantileSplit(bidDates: Seq[DateTime], clickDates: Seq[DateTime],
                         quantile: Double, relativeError: Double = 0.1) extends ClickTrackerSplit {
  override def logParams: Map[String, String] =
    Map(
      "dates" -> bidDates.map(formatDay).toString(),
      "quantile" -> f"$quantile%.2f",
      "relativeErr" -> f"$relativeError%.2f"
    )
}

/**
 * Benchmarks Features against predicting clicks at a global scale.
 * For benchmarking features at a finer granularity please look at [[ClickTrackerSwarmBenchmark]]
 */
class ClickTrackerGlobalBenchmark[M <: PredictionModel[Vector, M]]
    (estimator: Estimator[M], splitter: ClickTrackerSplit, paramGrid: Array[ParamMap])
  extends Estimator[M] with FitOnce {
  override val uid: String = Identifiable.randomUID("ClickTrackerGlobalBenchmark")

  final val featureCol = new Param[String](this, "featureCol", "The feature column")
  final val parallelism = new Param[Int](this, "parallelism", "Number of models to train in parallel")
  final val samplingRate: DoubleParam = new DoubleParam(this, "samplingRate",
    "Sampling rate for data (>= 0 && <= 1)", ParamValidators.inRange(0, 1))
  final val minimumCampaignClickRate: DoubleParam = new DoubleParam(this, "minimumCampaignClickRate",
    "Click rate rate for campaigns (>= 0 && <= 1)", ParamValidators.inRange(0, 1))
  final val seed = new Param[Long](this, "seed", "Seed for data sampling")
  final val mlFlowExperiment = new Param[Option[MlFlowExperiment]](this, "mlFlowExperiment", "MlFlow tracking experiment")
  final val clickTrackerDS = new Param[DateHourPartitionedDataset](this, "clickTrackerDS", "S3 dataset for clicktracker")
  final val bidFeedbackDS = new Param[DateHourPartitionedDataset](this, "bidFeedbackDS", "S3 dataset for bids")
  val metricName: Param[String] = {
    val allowedParams = ParamValidators.inArray(Array("areaUnderROC", "areaUnderPR"))
    new Param(
      this, "metricName", "metric name in evaluation (areaUnderROC|areaUnderPR)", allowedParams)
  }

  def setMetricName(value: String): this.type = set(metricName, value)
  setDefault(metricName -> "areaUnderPR") // PR curve focuses on the minority class
  def setFeatureCol(value: String): this.type = set(featureCol, value)
  setDefault(featureCol -> "featureCol")
  def setSamplingRate(value: Double): this.type = set(samplingRate, value)
  setDefault(samplingRate -> 1.0)
  def setMinimumCampaignClickRate(value: Double): this.type = set(minimumCampaignClickRate, value)
  setDefault(minimumCampaignClickRate -> 0.0)
  def setSeed(value: Long): this.type = set(seed, value)
  setDefault(seed -> 123)
  def setMlFlowExperiment(value: Option[MlFlowExperiment]): this.type = set(mlFlowExperiment, value)
  setDefault(mlFlowExperiment -> None)
  def setParallelism(value: Int): this.type = set(parallelism, value)
  setDefault(parallelism -> 2)
  def setClickTrackerDS(value: DateHourPartitionedDataset): this.type = set(clickTrackerDS, value)
  setDefault(clickTrackerDS -> ClickTracker)
  def setBidDS(value: DateHourPartitionedDataset): this.type = set(bidFeedbackDS, value)
  setDefault(bidFeedbackDS -> BidFeedback)

  def sampleBids(dataset: Dataset[_]): Dataset[_] = {
    if ($(samplingRate) < 1) dataset.sample($(samplingRate), $(seed)) else dataset
  }

  def joinBidsAndClicks(bids: Dataset[_], clicks: Dataset[_], sampleAvail:Boolean = true)
                       (implicit spark: SparkSession): Dataset[_] = {
    val sampledBids: Dataset[_] = sampleBids(bids)
      .select("BidRequestId", "CampaignId")

    val labelledClicks = clicks
      .select("BidRequestId", "CampaignId", "LogEntryTime")
      .withColumn("label", lit(1))

    val unfilteredBidsAndClicks = sampledBids
      .join(labelledClicks, Seq("BidRequestId", "CampaignId"), "leftouter")
      .na.fill(0)

    val mccr = $(minimumCampaignClickRate)
    if (mccr > 0) {
      val w = Window.partitionBy("CampaignId")
      unfilteredBidsAndClicks
        .withColumn("ctr", avg("label").over(w))
        .filter(col("ctr") > mccr)
        .drop("ctr")
    } else {
      unfilteredBidsAndClicks
    }
  }

  def joinFeaturesAndLabels(features:Dataset[_], labels: Dataset[_])(implicit spark: SparkSession): Dataset[_] = {
    labels
      .join(
        features.withColumnRenamed($(featureCol), "features"),
        "BidRequestId",
      )
  }

  private def estimatorAndDataset(ds: Dataset[_]): (TrainValidationSplitBenchmark[M], Dataset[_]) = {
    implicit val spark: SparkSession = ds.sparkSession
    val bidPath = $(bidFeedbackDS).readHours(_)
    val clickPath = $(clickTrackerDS).readHours(_)

    val (featuresAndLabels, customSplitter) = splitter match {
      case QuantileSplit(bidDates, clickDates, quantile, relativeError) =>

        val labels = joinBidsAndClicks(bidPath(bidDates), clickPath(clickDates))
          .withColumn("numericTime", unix_timestamp(col("LogEntryTime")))
        val fnl = joinFeaturesAndLabels(ds, labels)

        (fnl, ApproximateQuantileSplitter(quantile, relativeError, "numericTime"))
      case TrainTestDateSplit(bidTrainDates, bidTestDates, clickTrainDates, clickTestDates) =>

        val trainLabels = joinBidsAndClicks(bidPath(bidTrainDates), clickPath(clickTrainDates))
        val testLabels = joinBidsAndClicks(bidPath(bidTestDates), clickPath(clickTestDates))

        val fnlTrain = joinFeaturesAndLabels(ds, trainLabels)
        val fnlTest = joinFeaturesAndLabels(ds, testLabels)

        (fnlTrain, CustomValidationSplitter(fnlTest))
    }

    val benchmarkParams = Map(
      minimumCampaignClickRate.name -> "%.5f".format($(minimumCampaignClickRate)),
      samplingRate.name -> "%.3f".format($(samplingRate))
    )
    val experiment = $(mlFlowExperiment) match {
      case Some(MlFlowExperiment(name, uri, runParams)) =>
        Some(MlFlowExperiment(name, uri, runParams ++ splitter.logParams ++ benchmarkParams))
      case None =>
        None
    }

    val evaluator = new BinaryClassificationEvaluator().setMetricName($(metricName))
    val trainValSplitBenchmark = new TrainValidationSplitBenchmark[M]()
      .setEstimator(estimator)
      .setEvaluator(evaluator)
      .setMlFlowExperiment(experiment)
      .setEstimatorParamMaps(paramGrid)
      .setParallelism($(parallelism))
      .setSplitter(customSplitter)
      .setMetricName($(metricName))
    (trainValSplitBenchmark, featuresAndLabels)
  }

  override def fit(ds: Dataset[_]): M = {
    val (estimator, featuresAndLabels) = estimatorAndDataset(ds)
    estimator.fit(featuresAndLabels)
  }

  override def fitOnce(ds: Dataset[_]): (Array[Double], Array[ParamMap]) = {
    val (estimator, featuresAndLabels) = estimatorAndDataset(ds)
    estimator.fitOnce(featuresAndLabels)
  }

  override def copy(extra: ParamMap): Estimator[M] = defaultCopy(extra)

  override def transformSchema(schema: StructType): StructType = schema
}
