package com.ttd.features

import org.apache.spark.ml.{Pipeline, PipelineModel}
import org.apache.spark.sql.DataFrame
import org.json4s.jackson.Json

abstract class Feature[C <: FeatureConfig] {
  val featureName: String

  def config: C

  def createPipeline: Pipeline

  // Define main input to pipeline
  // also create temp views of other optional inputs
  def getPipelineInput: DataFrame

  def writePipelineOutput(dataFrame: DataFrame, pipelineModel: PipelineModel): Unit

  def getMetadata: Json = {
    throw new RuntimeException("Not yet Implemented")
  }

  def run: DataFrame = {
    val pipelineInput = getPipelineInput
    createPipeline.fit(pipelineInput).transform(pipelineInput)
  }
}