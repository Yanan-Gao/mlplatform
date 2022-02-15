package com.ttd.contextual.features.keywords.ngrams

import org.apache.spark.ml.Transformer
import org.apache.spark.ml.param.{IntParam, Param, ParamMap}
import org.apache.spark.ml.util.Identifiable
import org.apache.spark.sql.functions.{col, filter, slice}
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.{DataFrame, Dataset}

class FilterEmptyGrams extends Transformer {
  override val uid: String = Identifiable.randomUID("FilterEmptyGrams")

  final val gramCol = new Param[String](this, "gramCol", "The gram column")
  final val length = new IntParam(this, "lengh", "Number of grams to keep")

  def setGramCol(value: String): this.type = set(gramCol, value)
  setDefault(gramCol -> "grams")

  def setLength(value: Int): this.type = set(length, value)
  setDefault(length -> 100)

  override def transform(ds: Dataset[_]): DataFrame = {
    // slice first as an optimisation for large gram columns
    ds
      .withColumn($(gramCol),
        slice(
          filter(
            slice(col($(gramCol)), 1, $(length) * 2),
            x => x =!= ""
          ),
        1, $(length)
        )
      )

  }

  override def transformSchema(schema: StructType): StructType = {
    schema
  }

  override def copy(extra: ParamMap): FilterEmptyGrams = {
    defaultCopy(extra)
  }
}