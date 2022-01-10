package com.ttd.benchmarks.automl

import org.apache.spark.ThreadUtils
import org.apache.spark.internal.Logging
import org.apache.spark.ml.evaluation.Evaluator
import org.apache.spark.ml.param._
import org.apache.spark.ml.util.Identifiable
import org.apache.spark.ml.{Estimator, Model}
import org.apache.spark.sql.Dataset
import org.apache.spark.sql.types.StructType

import scala.concurrent.duration.Duration
import scala.concurrent.{ExecutionContext, Future}

/**
 * Trait that add the functionality for benchmarks to avoid training the best model again.
 * Normally,
 */
trait FitOnce {
  def fitOnce(dataset: Dataset[_]): (Array[Double], Array[ParamMap])
}

/**
 * Params for [[TrainValidationSplitBenchmark]].
 */
trait TrainValidationSplitParams extends Params {
  /**
   * Param that contains logic for splitting the input dataset into train and test sets
   */
  val splitter: Param[Splitter] = new Param(this, "splitter", "input dataset splitter logic")
  def getSplitter: Splitter = $(splitter)
  setDefault(splitter -> DefaultSplitter)

  /**
   * param for the estimator to be validated
   */
  val estimator: Param[Estimator[_]] = new Param(this, "estimator", "estimator for selection")
  def getEstimator: Estimator[_] = $(estimator)


  /**
   * param for estimator param maps
   */
  val estimatorParamMaps: Param[Array[ParamMap]] =
    new Param(this, "estimatorParamMaps", "param maps for the estimator")
  def getEstimatorParamMaps: Array[ParamMap] = $(estimatorParamMaps)

  /**
   * param for the evaluator used to select hyper-parameters that maximize the validated metric
   */
  val evaluator: Param[Evaluator] = new Param(this, "evaluator",
    "evaluator used to select hyper-parameters that maximize the validated metric")
  def getEvaluator: Evaluator = $(evaluator)

  protected def transformSchemaImpl(schema: StructType): StructType = {
    require($(estimatorParamMaps).nonEmpty, s"Validator requires non-empty estimatorParamMaps")
    val firstEstimatorParamMap = $(estimatorParamMaps).head
    val est = $(estimator)
    for (paramMap <- $(estimatorParamMaps).tail) {
      est.copy(paramMap).transformSchema(schema)
    }
    est.copy(firstEstimatorParamMap).transformSchema(schema)
  }
}

trait HasParallelism extends Params {

  /**
   * The number of threads to use when running parallel algorithms.
   * Default is 1 for serial execution
   *
   * @group expertParam
   */
  val parallelism = new IntParam(this, "parallelism",
    "the number of threads to use when running parallel algorithms", ParamValidators.gtEq(1))

  setDefault(parallelism -> 1)

  /** @group expertGetParam */
  def getParallelism: Int = $(parallelism)

  /**
   * Create a new execution context with a thread-pool that has a maximum number of threads
   * set to the value of [[parallelism]]. If this param is set to 1, a same-thread executor
   * will be used to run in serial.
   */
  def getExecutionContext: ExecutionContext = {
    getParallelism match {
      case 1 =>
        ThreadUtils.sameThread
      case n =>
        ExecutionContext.fromExecutorService(ThreadUtils
          .newDaemonCachedThreadPool(s"${this.getClass.getSimpleName}-thread-pool", n))
    }
  }
}

/**
 * Validation for hyper-parameter tuning.
 * By default will randomly splits the input dataset into train and validation sets,
 * and uses evaluation metric on the validation set to select the best model.
 * Similar to [[CrossValidatorBenchmark]], but only splits the set once.
 */
class TrainValidationSplitBenchmark[M <: Model[M]](override val uid: String) extends Estimator[M]
    with TrainValidationSplitParams with HasParallelism
    with HasMlFlowExperiment with Logging with FitOnce {

  def this() = this(Identifiable.randomUID("TrainValidationSplitMlFlow"))

  final val metricName: Param[String] = new Param(this, "metricName", "metric name in evaluation")
  setDefault(metricName -> "unknown_metric")

  def setMetricName(value: String): this.type = set(metricName, value)
  def setEstimator(value: Estimator[_]): this.type = set(estimator, value)
  def setEstimatorParamMaps(value: Array[ParamMap]): this.type = set(estimatorParamMaps, value)
  def setEvaluator(value: Evaluator): this.type = set(evaluator, value)
  def setSplitter(value: Splitter): this.type = set(splitter, value)
  def setMlFlowExperiment(value: Option[MlFlowExperiment]): this.type = set(mlFlowExperiment, value)

  /**
   * Set the maximum level of parallelism to evaluate models in parallel.
   * Default is 1 for serial evaluation
   */
  def setParallelism(value: Int): this.type = set(parallelism, value)

  override def fit(dataset: Dataset[_]): M = {
    val (metrics, epm) = fitOnce(dataset)
    val (bestMetric, bestIndex) =
      if ($(evaluator).isLargerBetter) metrics.zipWithIndex.maxBy(_._1)
      else metrics.zipWithIndex.minBy(_._1)
    log.info(s"Best set of parameters:\n${epm(bestIndex)}")
    log.info(s"Best train validation split metric: $bestMetric.")
    val bestModel = $(estimator).fit(dataset, epm(bestIndex)).asInstanceOf[M]
    bestModel
  }

  /**
   * Fits the estimator to each paramMap once and does not re-train the best model at the end
   */
  override def fitOnce(dataset: Dataset[_]): (Array[Double], Array[ParamMap]) = {
    val schema = dataset.schema
    transformSchema(schema, logging = true)
    val est = $(estimator)
    val eval = $(evaluator)
    val epm = $(estimatorParamMaps)

    // Create execution context based on $(parallelism)
    val executionContext = getExecutionContext

    // Split the input into train and validation sets
    val TrainValDataset(trainingDataset, validationDataset) = $(splitter).split(dataset)
    trainingDataset.cache()
    validationDataset.cache()

    // Fit models in a Future for training in parallel
    log.debug(s"Train split with multiple sets of parameters.")

    // create experiment in main thread so there is no race condition
    val experimentId = createExperiment

    val metricFutures = epm.zipWithIndex.map { case (paramMap, _) =>
      Future[Double] {

        lazy val fitAndEval: (Double, Double) = {
          val model = est.fit(trainingDataset, paramMap).asInstanceOf[Model[_]]

          val train_metric: Double = eval.evaluate(model.transform(trainingDataset, paramMap))
          val val_metric: Double = eval.evaluate(model.transform(validationDataset, paramMap))
          (train_metric, val_metric)
        }

        // log metric to mlflow if mlFlowExperiment was created
        experimentId match {
          case Some(id) =>
            val mlflowContext = getExperimentContext(id)
            val run = mlflowContext.startRun(Identifiable.randomUID("run") )
            val (train_metric, val_metric) = fitAndEval
            run.logParam("val_" + $(metricName), val_metric.toString)
            run.logParam("train_" + $(metricName), train_metric.toString)
            run.logParam("class", est.getClass.toString)
            logRunParams(run)
            paramMap.toSeq.foreach(paramPair => run.logParam(paramPair.param.name, paramPair.value.toString))
            run.endRun()
            val_metric
          case None =>
            val (train_metric, val_metric) = fitAndEval
            log.info(s"Got val metric $val_metric & train metric $train_metric for model trained with $paramMap.")
            val_metric
        }
      } (executionContext)
    }

    // Wait for all metrics to be calculated
    val metrics = metricFutures.map(ThreadUtils.awaitResult(_, Duration.Inf))

    // Unpersist training & validation set once all metrics have been produced
    trainingDataset.unpersist()
    validationDataset.unpersist()

    log.info(s"Train validation split metrics: ${metrics.toSeq}")
    (metrics, epm)
  }

  override def transformSchema(schema: StructType): StructType = transformSchemaImpl(schema)

  override def copy(extra: ParamMap): TrainValidationSplitBenchmark[M] = {
    val copied = defaultCopy(extra).asInstanceOf[TrainValidationSplitBenchmark[M]]
    if (copied.isDefined(estimator)) {
      copied.setEstimator(copied.getEstimator.copy(extra))
    }
    if (copied.isDefined(evaluator)) {
      copied.setEvaluator(copied.getEvaluator.copy(extra))
    }
    copied
  }
}