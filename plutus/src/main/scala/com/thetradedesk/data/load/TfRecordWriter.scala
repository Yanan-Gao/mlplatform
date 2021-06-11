package com.thetradedesk.data.load

import org.apache.spark.sql.SaveMode
import org.apache.spark.ml.feature.FeatureHasher
import org.apache.spark.sql.{Column, DataFrame}

object TfRecordWriter {

  // hashing trick hashing -> not currently in use, keeping in case we pull it out of retirement
  def hashData(df: DataFrame, inputCols: Seq[String], dims: Int) = {

    val hasher = new FeatureHasher()
      .setNumFeatures(dims)
      .setInputCols(inputCols.toArray)
      .setCategoricalCols(inputCols.toArray)
      .setOutputCol("features")
    hasher.transform(df)

  }

  def writeData(df: DataFrame, selection: Array[Column], outputPath: String): Unit = {
    df
      .select(selection: _*)
      .repartition(75)
      .write
      .mode(SaveMode.Overwrite)
      .format("tfrecord")
      .option("recordType", "Example")
      .option("codec", "org.apache.hadoop.io.compress.GzipCodec")
      .save(outputPath)

  }


}
