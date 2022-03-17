package com.ttd.features.util

import org.apache.spark.ml.Pipeline
import org.apache.spark.sql.{DataFrame, Dataset}

object PipelineUtils {
  implicit class WrappedPipeline(val pipe: Pipeline) extends AnyVal {
    def fitTransform(dataset: Dataset[_]): DataFrame = {
      pipe.fit(dataset).transform(dataset)
    }
  }
}
