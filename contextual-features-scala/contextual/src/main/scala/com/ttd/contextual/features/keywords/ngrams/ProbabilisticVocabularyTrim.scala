package com.ttd.contextual.features.keywords.ngrams

import org.apache.spark.broadcast.Broadcast
import org.apache.spark.ml.Transformer
import org.apache.spark.ml.param.{IntParam, Param, ParamMap}
import org.apache.spark.ml.feature.{CountVectorizer, CountVectorizerModel}
import org.apache.spark.ml.util.Identifiable
import org.apache.spark.sql.functions.{col, explode, filter, slice, udf}
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.{DataFrame, Dataset}
import org.apache.spark.util.collection.OpenHashMap
import org.apache.spark.util.sketch.BloomFilter

import scala.collection.mutable

class ProbabilisticVocabularyTrim extends Transformer {
  override val uid: String = Identifiable.randomUID("ProbabilisticVocabularyTrim")

  final val gramCol = new Param[String](this, "gramCol", "The gram column")
  final val length = new IntParam(this, "length", "Number of grams to keep")
  final val vocabSize = new IntParam(this, "vocabSize", "Vocab size")

  def setGramCol(value: String): this.type = set(gramCol, value)
  setDefault(gramCol -> "grams")

  def setLength(value: Int): this.type = set(length, value)
  setDefault(length -> 100)

  def setVocabSize(value: Int): this.type = set(vocabSize, value)
  setDefault(vocabSize -> 10000000)

  override def transform(ds: Dataset[_]): DataFrame = {
    import ds.sparkSession.implicits._

    val bloomFilter = ds.sparkSession
      .table("vocabulary")
      .select(explode(col($(gramCol))) as "gram")
      .stat.bloomFilter("gram", $(vocabSize), 0.001)

    // how to get vocab trimmed for benchmark:
    // or how to trim before url history join!

    val vocab = ds
      .select("grams")
      .as[Seq[String]]
      .rdd
      .flatMap { tokens =>
        tokens
          .distinct
          .map(word => (word, 1))
      }.reduceByKey { (wcdf1, wcdf2) => (wcdf1 + wcdf2)}
      .top(1000)(Ordering.by(_._2)) // ordered by document count
      .map(_._1)
      .toSeq
      .toDF()
      .stat.bloomFilter("gram", $(vocabSize), 0.001)


    val bc: Broadcast[BloomFilter] = ds.sparkSession.sparkContext.broadcast(bloomFilter)

    val bloomHistoryFilter = udf((grams: Seq[String]) => {
      grams.filter(bc.value.mightContainString)
    })

    ds
      .withColumn("grams", bloomHistoryFilter(col("grams")))

  }

  override def transformSchema(schema: StructType): StructType = {
    schema
  }

  override def copy(extra: ParamMap): ProbabilisticVocabularyTrim = {
    defaultCopy(extra)
  }
}