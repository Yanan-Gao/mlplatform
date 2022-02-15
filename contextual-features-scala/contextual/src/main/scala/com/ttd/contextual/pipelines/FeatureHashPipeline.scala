package com.ttd.contextual.pipelines

import org.apache.spark.ml.Pipeline
import org.apache.spark.ml.clustering.DistributedLDAModel
import org.apache.spark.ml.feature.{OneHotEncoder, CountVectorizer, FeatureHasher, HashingTF, IDF, VectorAssembler, StringIndexer}

object FeatureHashPipeline {

  def countFeatures(
                    inputCol: String,
                    outputCol: String,
                    otherCols: Seq[String],
                    numFeatures: Int = 10000,
                    binary: Boolean = false
                  ): Pipeline = {
    val tf = new CountVectorizer()
      .setInputCol(inputCol)
//      .setVocabSize(100000)
      .setOutputCol("tf")
      .setBinary(binary)

    val idf = new IDF()
      .setInputCol("tf")
      .setOutputCol("tf_idf")

    val assembler = new VectorAssembler()
      .setInputCols(Array("tf_idf") ++ otherCols)
      .setOutputCol(outputCol)

    new Pipeline().setStages(
      Array(
        tf,
        idf,
        assembler
      )
    )
  }

  def hashFeatures(
                    inputCol: String,
                    outputCol: String,
                    otherCols: Seq[String],
                    numFeatures: Int = 10000,
                    binary: Boolean = false
                  ): Pipeline = {

    val tf = new HashingTF()
      .setInputCol(inputCol)
      .setOutputCol("tf")
      .setBinary(binary)
      .setNumFeatures(numFeatures)

    val idf = new IDF()
      .setInputCol("tf")
      .setOutputCol("tf_idf")

    val assembler = new VectorAssembler()
      .setInputCols(Array("tf_idf") ++ otherCols)
      .setOutputCol(outputCol)

    new Pipeline().setStages(
      Array(
        tf,
        idf,
        assembler
      )
    )
  }
}
