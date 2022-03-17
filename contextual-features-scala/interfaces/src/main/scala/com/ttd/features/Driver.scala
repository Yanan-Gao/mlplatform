package com.ttd.features

import org.apache.spark.ml.{Pipeline, PipelineModel}
import org.apache.spark.sql.DataFrame
import org.json4s.jackson.Json

abstract class Driver[C <: FeatureConfig] extends Runnable {
  val config: C
}

trait MainRunnable[D <: Driver[_]] {
  val driver: D

  def main(args: Array[String]): Unit = {
    driver.run()
  }
}