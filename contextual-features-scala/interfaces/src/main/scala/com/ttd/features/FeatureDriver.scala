package com.ttd.features

trait FeatureDriver[D <: Feature[_]] {
  val driver: D

  def main(args: Array[String]): Unit = {
    val pipelineInput = driver.getPipelineInput
    val pipeline = driver.createPipeline
    val model = pipeline.fit(pipelineInput)
    val output = model.transform(pipelineInput)
    driver.writePipelineOutput(output, model)
  }
}