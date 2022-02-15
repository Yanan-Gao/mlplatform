package com.ttd.contextual.features.keywords.ngrams

import org.apache.spark.ml.{Transformer, UnaryTransformer}
import org.apache.spark.ml.param.{IntParam, Param, ParamMap, ParamValidators}
import org.apache.spark.ml.util.{DefaultParamsWritable, Identifiable}
import org.apache.spark.sql.{DataFrame, Dataset}
import org.apache.spark.sql.functions.{col, filter, slice, udf}
import org.apache.spark.sql.types.{ArrayType, DataType, StringType, StructType}

class NGram(override val uid: String) extends Transformer {
  def this() = {
    this(Identifiable.randomUID("NGram"))
  }

  /**
   * Minimum n-gram length, greater than or equal to 1.
   * Default: 2, bigram features
   * @group param
   */
  val n: IntParam = new IntParam(this, "n", "number elements per n-gram (>=1)",
    ParamValidators.gtEq(1))

  def setN(value: Int): this.type = set(n, value)

  def getN: Int = $(n)

  setDefault(n -> 2)

  final val gramCol = new Param[String](this, "gramCol", "The gram column")

  def setGramCol(value: String): this.type = set(gramCol, value)
  setDefault(gramCol -> "grams")

  def toNGrams(tokens: Seq[String]): Seq[String] = {
    tokens.iterator
      .sliding($(n))
      .withPartial(false)
      .map(_.mkString(" "))
      .toSeq
  }

  override def transform(ds: Dataset[_]): DataFrame = {
    val toNGramsUDF = udf((toks: Seq[String]) => toNGrams(toks))

    ds
      .withColumn($(gramCol), toNGramsUDF(col($(gramCol))))
  }

  override def transformSchema(schema: StructType): StructType = {
    schema
  }

  override def copy(extra: ParamMap): NGram = {
    defaultCopy(extra)
  }
}