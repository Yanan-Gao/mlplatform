package com.ttd.contextual.features.keywords.ngrams

import com.ttd.contextual.spark.TTDSparkContext.spark.implicits._
import org.apache.spark.ml.Transformer
import org.apache.spark.ml.param.{IntParam, Param, ParamMap}
import org.apache.spark.ml.util.Identifiable
import org.apache.spark.sql.functions.{col, udf}
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.{DataFrame, Dataset}

class TrimVocabulary extends Transformer {
  override val uid: String = Identifiable.randomUID("FilterEmptyGrams")

  final val gramCol = new Param[String](this, "gramCol", "The gram column")
  final val vocabSize = new IntParam(this, "vocabSize", "Number of grams to keep")

  def setGramCol(value: String): this.type = set(gramCol, value)
  setDefault(gramCol -> "grams")

  def setLength(value: Int): this.type = set(vocabSize, value)
  setDefault(vocabSize -> 10000000)

  override def transform(ds: Dataset[_]): DataFrame = {
    val vocabulary = ds
      .select(col($(gramCol)) as $(gramCol))
      .as[Seq[String]]
      .rdd
      .flatMap { tokens =>
        tokens
          .distinct
          .map(word => (word, 1))
      }.reduceByKey { (wcdf1, wcdf2) => (wcdf1 + wcdf2)}
      .top($(vocabSize))(Ordering.by(_._2)) // ordered by document count
      .map(_._1)
      .toSet

    // todo: could make this a bloom filter for large vocabularies
    val broadcastVocab = ds.sparkSession.sparkContext.broadcast(vocabulary)
    val vocabFilterUDF = udf((tokens: Seq[String]) => {
      tokens.filter(broadcastVocab.value.contains)
    })

    ds
      .withColumn($(gramCol), vocabFilterUDF(col($(gramCol))))
  }

  override def transformSchema(schema: StructType): StructType = {
    schema
  }

  override def copy(extra: ParamMap): this.type = {
    defaultCopy(extra)
  }
}