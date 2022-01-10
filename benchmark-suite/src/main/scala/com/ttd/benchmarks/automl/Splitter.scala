package com.ttd.benchmarks.automl

import org.apache.spark.sql.Dataset
import org.apache.spark.sql.functions.col


case class TrainValDataset(train: Dataset[_], validation: Dataset[_])

/**
 * Custom Splitter class
 */
trait Splitter {
  def split(dataset: Dataset[_]): TrainValDataset
}

/**
 * Default splitter splits dataset randomly according to the trainRatio
 */
case class RandomSplitter(trainRatio: Double, seed: Long) extends Splitter {
  override def split(dataset: Dataset[_]): TrainValDataset = {
    val Array(train, validation) = dataset.randomSplit(Array(trainRatio, 1 - trainRatio), seed)
    TrainValDataset(train = train, validation = validation)
  }
}

/**
 * Splitter that uses the same dataset for train and validation
 */
object NoSplit extends Splitter {
  override def split(dataset: Dataset[_]): TrainValDataset = {
    TrainValDataset(train = dataset, validation = dataset)
  }
}

/**
 * Custom splitter that takes a predefined validation dataset
 */
case class CustomValidationSplitter(validation: Dataset[_]) extends Splitter {
  override def split(dataset: Dataset[_]): TrainValDataset = {
    TrainValDataset(train = dataset, validation = validation)
  }
}

/**
 * Splitter that approximately splits a dataset using a numeric column
 */
case class ApproximateQuantileSplitter(quantile: Double, relativeError: Double = 0.1, numericCol: String) extends Splitter {
  override def split(dataset: Dataset[_]): TrainValDataset = {
    val Array(trainSplitTime) = dataset.stat.approxQuantile(numericCol, Array(quantile), relativeError)
    val train = dataset.filter(col(numericCol) < trainSplitTime)
    val validation = dataset.filter(col(numericCol) >= trainSplitTime)
    TrainValDataset(train = train, validation = validation)
  }
}

object DefaultSplitter extends RandomSplitter(trainRatio = 0.8, seed = 123L)
