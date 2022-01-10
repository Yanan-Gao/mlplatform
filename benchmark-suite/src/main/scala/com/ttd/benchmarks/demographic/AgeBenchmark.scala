package com.ttd.benchmarks.demographic

import com.ttd.benchmarks.automl.{Benchmark, DefaultSplitter, MlFlowExperiment, Splitter}
import com.ttd.benchmarks.util.logging.{AgeAndGenderLabels, RegionDatePartitionedDataset, S3Paths}
import org.apache.spark.ml.Estimator
import org.apache.spark.ml.evaluation.{Evaluator, RegressionEvaluator}
import org.apache.spark.ml.linalg.Vector
import org.apache.spark.ml.param.{DoubleParam, Param, ParamMap, ParamValidators}
import org.apache.spark.ml.regression.RegressionModel
import org.apache.spark.ml.util.Identifiable
import org.apache.spark.sql.Dataset
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.types.StructType
import org.joda.time.DateTime

class AgeBenchmark[M <: RegressionModel[Vector, M]]
    (estimator: Estimator[M], dates: Seq[DateTime], paramGrid: Array[ParamMap]) extends Estimator[M] {
  override val uid: String = Identifiable.randomUID("AgeBenchmark")

  final val joinCol = new Param[String](this, "joinCol", "The join column")
  final val featureCol = new Param[String](this, "featureCol", "The feature column")
  final val region = new Param[String](this, "region", "Region")
  final val trainSplitOrNFolds = new Param[Either[Splitter, Int]](this, "trainSplitOrNFolds", "Train split or cross val with N folds")
  final val parallelism = new Param[Int](this, "parallelism", "Number of models to train in parallel")
  final val samplingRate: DoubleParam = new DoubleParam(this, "samplingRate",
    "Sampling rate for data (>= 0 && <= 1)", ParamValidators.inRange(0, 1))
  final val seed = new Param[Long](this, "seed", "Seed for data sampling")
  final val mlFlowExperiment = new Param[Option[MlFlowExperiment]](this, "mlFlowExperiment", "MlFlow tracking experiment")
  final val dateRegionLabelsDS = new Param[RegionDatePartitionedDataset](this, "dateRegionLabelsDS", "S3 path builder from date and region")
  final val metricName: Param[String] = {
    val allowedParams = ParamValidators.inArray(Array("mse", "rmse", "r2", "mae", "var"))
    new Param(this, "metricName", "metric name in evaluation (mse|rmse|r2|mae|var)", allowedParams)
  }

  def setMetricName(value: String): this.type = set(metricName, value)
  setDefault(metricName -> "rmse")
  def setJoinKey(value: String): this.type = set(joinCol, value)
  setDefault(joinCol -> "userId")
  def setFeatureCol(value: String): this.type = set(featureCol, value)
  setDefault(featureCol -> "featureCol")
  def setRegion(value: String): this.type = set(region, value)
  setDefault(region -> "JP")
  def setSamplingRate(value: Double): this.type = set(samplingRate, value)
  setDefault(samplingRate -> 1.0)
  def setSeed(value: Long): this.type = set(seed, value)
  setDefault(seed -> 123)
  def setMlFlowExperiment(value: Option[MlFlowExperiment]): this.type = set(mlFlowExperiment, value)
  setDefault(mlFlowExperiment -> None)
  def setTrainOrSplit(value: Either[Splitter, Int]): this.type = set(trainSplitOrNFolds, value)
  setDefault(trainSplitOrNFolds -> Left(DefaultSplitter))
  def setParallelism(value: Int): this.type = set(parallelism, value)
  setDefault(parallelism -> 2)
  def setDateRegionDs(value: RegionDatePartitionedDataset): this.type = set(dateRegionLabelsDS, value)
  setDefault(dateRegionLabelsDS -> AgeAndGenderLabels)

  override def fit(ds: Dataset[_]): M = {
    import ds.sparkSession.implicits._

    var ageLabels = $(dateRegionLabelsDS)
      .readRegionDates($(region), dates)(ds.sparkSession)
      .select($"userId" as $(joinCol), $"ageLabel" as "label")

    ageLabels = (if ($(samplingRate) < 1) ageLabels.sample($(samplingRate), $(seed)) else ageLabels)

    val labelCount = ageLabels.count
    val featuresAndLabels = ageLabels
      .join(
        ds.select(col($(featureCol)) as "features", col($(joinCol))),
        Seq($(joinCol))
      )
    val joinedCount = featuresAndLabels.count

    val logParams = Map("coverage" -> "%.3f".format(joinedCount.toDouble / labelCount.toDouble))

    val experiment = $(mlFlowExperiment) match {
      case Some(MlFlowExperiment(name, uri, runParams)) =>
        Some(MlFlowExperiment(name, uri, runParams ++ logParams))
      case None =>
        None
    }

    val regressionBenchmark = new Benchmark[M](estimator, paramGrid, new RegressionEvaluator().setMetricName($(metricName)))
      .setTrainSplitOrNFolds($(trainSplitOrNFolds))
      .setParallelism($(parallelism))
      .setMlFlowExperiment(experiment)
      .setMetricName($(metricName))
      .setSeed($(seed))

    regressionBenchmark.fit(featuresAndLabels)
  }

  override def copy(extra: ParamMap): Estimator[M] = defaultCopy(extra)

  override def transformSchema(schema: StructType): StructType = schema
}
