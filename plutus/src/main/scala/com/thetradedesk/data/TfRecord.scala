package com.thetradedesk.data

import java.time.LocalDate

import job.TrainDataProcessor.{date, dims, inputCatCols, inputIntCols, outputPath, rawCols, targets}
import org.apache.spark.ml.feature.FeatureHasher
import org.apache.spark.ml.linalg.{SparseVector, Vector}
import org.apache.spark.sql.{Column, DataFrame}
import org.apache.spark.sql.functions.{col, udf}

class TfRecord {

  def hashData(df: DataFrame, inputCols: Seq[String]) = {

    val hasher = new FeatureHasher()
      .setNumFeatures(dims)
      .setInputCols(inputCols.toArray)
      .setCategoricalCols(inputCols.toArray)
      .setOutputCol("features")
    hasher.transform(df)

  }

  def writeData(date: LocalDate, folderName:String, tfRecordPath: String, outputType: String, df: DataFrame, selection: Array[Column]): Unit = {

    val d = datePaddedPart(date)

    df
      .select(selection: _*)
      .repartition(75)
      .write.format("tfrecords").option("recordType", "Example")
      .option("codec", "org.apache.hadoop.io.compress.GzipCodec")
      .mode("overwrite").save(outputPath + folderName + tfRecordPath + outputType + "/" + d)

  }


}
