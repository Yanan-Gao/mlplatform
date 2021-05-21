package com.thetradedesk.data.load

import com.thetradedesk.data.paddedDatePart
import org.apache.spark.ml.feature.FeatureHasher
import org.apache.spark.sql.{Column, DataFrame}

import java.time.LocalDate

object TfRecordWriter {

  def hashData(df: DataFrame, inputCols: Seq[String], dims: Int) = {

    val hasher = new FeatureHasher()
      .setNumFeatures(dims)
      .setInputCols(inputCols.toArray)
      .setCategoricalCols(inputCols.toArray)
      .setOutputCol("features")
    hasher.transform(df)

  }

  def writeData(df: DataFrame, selection: Array[Column], date: LocalDate, outputPath: String, folderName: String, ttdEnv: String, tfRecordPath: String, outputType: String): Unit = {

    val d = paddedDatePart(date)

    df
      .select(selection: _*)
      .repartition(75)
      .write.format("tfrecords").option("recordType", "Example")
      .option("codec", "org.apache.hadoop.io.compress.GzipCodec")
      .mode("overwrite")
      .save(outputPath + folderName + "/" + ttdEnv + tfRecordPath + outputType + "/" + d)

  }


}
