package com.ttd.contextual.features.keywords.yake

import com.ttd.contextual.spark.TTDSparkContext.spark.implicits._
import org.apache.spark.ml.Transformer
import org.apache.spark.ml.param.{Param, ParamMap}
import org.apache.spark.ml.util.Identifiable
import org.apache.spark.sql.functions.{array_distinct, arrays_zip, col}
import org.apache.spark.sql.types._
import org.apache.spark.sql.{DataFrame, Dataset, functions}

// Summarizes output from [[YakeKeywordExtraction]]
class YakeKeywordFinisher extends Transformer {
  override val uid: String = Identifiable.randomUID("YakeKeywordFinisher")

  final val yakeCol = new Param[String](this, "yakeCol", "The output from yake model")
  final val keyphraseCol = new Param[String](this, "keyphraseCol", "The output keyphrases")
  final val scoresCol = new Param[String](this, "scoresCol", "The output keyphrase scores")

  def setYakeCol(value: String): this.type = set(yakeCol, value)
  setDefault(yakeCol -> "keywords")
  def setKeyphraseCol(value: String): this.type = set(keyphraseCol, value)
  setDefault(keyphraseCol -> "keyphrases")
  def setScoresCol(value: String): this.type = set(scoresCol, value)
  setDefault(scoresCol -> "scores")

  override def transform(ds: Dataset[_]): DataFrame = {
    val results = $(yakeCol) + ".result"
    val metadata = $(yakeCol) + ".metadata"
    ds
      .withColumn("_tempKeyphrases",
        array_distinct(arrays_zip(col(results), functions.transform(col(metadata), c => c.getItem("score").cast("float")))))
      .withColumn($(keyphraseCol), $"_tempKeyphrases.result")
      .withColumn($(scoresCol), $"_tempKeyphrases.1")
      .drop("_tempKeyphrases")
  }

  override def transformSchema(schema: StructType): StructType = {
    schema
      .add(StructField($(keyphraseCol), ArrayType(StringType), nullable = false))
      .add(StructField($(scoresCol), ArrayType(FloatType), nullable = false))
  }

  override def copy(extra: ParamMap): YakeKeywordFinisher = {
    defaultCopy(extra)
  }
}
