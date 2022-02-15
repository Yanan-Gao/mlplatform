package com.ttd.features.transformers

import org.apache.spark.ml.{Pipeline, PipelineStage, Transformer}
import org.apache.spark.ml.param.{Param, ParamMap}
import org.apache.spark.ml.util.{DefaultParamsReadable, DefaultParamsWritable, Identifiable}
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.{DataFrame, Dataset}


object IF {
  def apply(condition: Boolean, _then: Transformer): Pipeline = {
    if (condition) {
      new Pipeline().setStages(Array(_then))
    } else {
      new Pipeline().setStages(Array.empty[PipelineStage])
    }
  }

  def apply(condition: Boolean, _then: Transformer, otherwise: Transformer): Pipeline = {
    if (condition) {
      new Pipeline().setStages(Array(_then))
    } else {
      new Pipeline().setStages(Array(otherwise))
    }
  }
}
