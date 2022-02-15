package com.ttd.features.transformers

import org.apache.spark.ml.Transformer
import org.apache.spark.ml.param.{Param, ParamMap}
import org.apache.spark.ml.util.{DefaultParamsReadable, DefaultParamsWritable, Identifiable}
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.{Column, DataFrame, Dataset}

// DefaultParamsWritable assumes this constructor is available
class Filter(override val uid: String)
  extends Transformer with DefaultParamsWritable {

  def this() = {
    this(Identifiable.randomUID("Filter"))
  }

  final val conditionParam = new Param[String](this, "condition", "The filter condition")

  override def transform(ds: Dataset[_]): DataFrame = {
    ds.filter($(conditionParam)).toDF()
  }

  override def transformSchema(schema: StructType): StructType = {
    schema
  }

  override def copy(extra: ParamMap): this.type = {
    defaultCopy(extra)
  }
}

object Filter extends DefaultParamsReadable[Filter] {
  def apply(condition: Column): Filter = {
    val filter = new Filter()
    filter.set(filter.conditionParam -> condition.expr.sql)
  }
}
