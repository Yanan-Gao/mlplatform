package com.ttd.benchmarks.automl

import org.apache.spark.ml.param.{Param, Params}
import org.mlflow.tracking.{ActiveRun, MlflowContext}

import scala.collection.JavaConverters._

case class MlFlowExperiment(experiment: String, trackingUri: Option[String] = None, runParams: Map[String, String] = Map.empty)

trait HasMlFlowExperiment extends Params {
  final val mlFlowExperiment = new Param[Option[MlFlowExperiment]](this, "mlFlowExperiment", "MlFlow tracking experiment")
  setDefault(mlFlowExperiment -> None)

  def getTrackingUri: Option[MlFlowExperiment] = $(mlFlowExperiment)

  /**
   * Gets or creates a new experiment with experiment name
   * @return The created experimentId
   */
  def createExperiment: Option[String] = {
    $(mlFlowExperiment) match {
      case Some(MlFlowExperiment(experimentName, Some(uri), _)) =>
        val mlflowContext = new MlflowContext(uri)
        val client = mlflowContext.getClient

        val experimentOpt = client.getExperimentByName(experimentName)
        if (!experimentOpt.isPresent) {
          Some(client.createExperiment(experimentName))
        } else {
          Some(experimentOpt.get.getExperimentId)
        }
      case Some(MlFlowExperiment(experimentName, None, _)) =>
        val mlflowContext = new MlflowContext()
        val client = mlflowContext.getClient
        val experimentOpt = client.getExperimentByName(experimentName)
        if (!experimentOpt.isPresent) {
          Some(client.createExperiment(experimentName))
        } else {
          Some(experimentOpt.get.getExperimentId)
        }
      case None =>
        None
    }
  }

  def getExperimentContext(experimentId: String): MlflowContext = {
    $(mlFlowExperiment) match {
      case Some(MlFlowExperiment(_, Some(uri),_)) =>
        val mlflowContext = new MlflowContext(uri)
        mlflowContext.setExperimentId(experimentId)
        mlflowContext
      case Some(MlFlowExperiment(_, None,_)) =>
        val mlflowContext = new MlflowContext()
        mlflowContext.setExperimentId(experimentId)
        mlflowContext
      case None =>
        throw new RuntimeException("mlFlowExperiment parameter not provided")
    }
  }

  def logRunParams(activeRun: ActiveRun):Unit = {
    $(mlFlowExperiment) match {
      case Some(MlFlowExperiment(_,_,runParams)) =>
        activeRun.logParams(runParams.asJava)
      case _ =>
    }
  }
}
