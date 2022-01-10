package com.ttd.benchmarks.automl

import org.apache.spark.ml.evaluation.Evaluator
import org.apache.spark.ml.linalg.Vector
import org.apache.spark.ml.param.{Param, ParamMap}
import org.apache.spark.ml.util.Identifiable
import org.apache.spark.ml.{Estimator, PredictionModel}
import org.apache.spark.sql.Dataset
import org.apache.spark.sql.types.StructType


class Benchmark[M <: PredictionModel[Vector, M]]
  (estimator: Estimator[_], paramGrid: Array[ParamMap], evaluator: Evaluator)
  extends Estimator[M] {
  override val uid: String = Identifiable.randomUID("RegressionBenchmark")

  // Evaluate up to `parallelism` parameter settings in parallel
  final val parallelism = new Param[Int](this, "parallelism", "Number of models to train concurrently")
  final val trainSplitOrNFolds = new Param[Either[Splitter, Int]](this, "trainSplitOrNFolds", "Use a train split or N fold cross validation")
  final val seed = new Param[Long](this, "seed", "Seed for splits")
  final val mlFlowExperiment = new Param[Option[MlFlowExperiment]](this, "mlFlowExperiment", "MlFlow tracking experiment")
  final val metricName: Param[String] = new Param(this, "metricName", "metric name in evaluation")

  def setMetricName(value: String): this.type = set(metricName, value)
  setDefault(metricName -> "unknown_metric")

  def setMlFlowExperiment(value: Option[MlFlowExperiment]): this.type = set(mlFlowExperiment, value)
  setDefault(mlFlowExperiment -> None)

  def setTrainSplitOrNFolds(value: Either[Splitter, Int]): this.type = set(trainSplitOrNFolds, value)
  setDefault(trainSplitOrNFolds -> Left(DefaultSplitter))

  def setParallelism(value: Int): this.type = set(parallelism, value)
  setDefault(parallelism -> 2)

  def setSeed(value: Long): this.type = set(seed, value)
  setDefault(seed -> 123)

  override def fit(dataset: Dataset[_]): M = {
  val splitOrFold: M = $(trainSplitOrNFolds) match {
    case Left(splitter) =>
      new TrainValidationSplitBenchmark()
        .setEstimator(estimator)
        .setEvaluator(evaluator)
        .setMlFlowExperiment($(mlFlowExperiment))
        .setEstimatorParamMaps(paramGrid)
        .setParallelism($(parallelism))
        .setSplitter(splitter)
        .setMetricName($(metricName))
        .fit(dataset)
    case Right(numFolds) =>
      // TODO cross val with MlFlow
      throw new RuntimeException("Not yet implemented")
  }

    splitOrFold
  }

  override def copy(extra: ParamMap): Estimator[M] = defaultCopy(extra)

  override def transformSchema(schema: StructType): StructType = schema
}
