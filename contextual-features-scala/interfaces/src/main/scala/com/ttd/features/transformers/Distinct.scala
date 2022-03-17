package com.ttd.features.transformers

import org.apache.spark.ml.Transformer
import org.apache.spark.ml.param.{ParamMap, StringArrayParam}
import org.apache.spark.ml.util.{DefaultParamsReadable, DefaultParamsWritable, Identifiable}
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.{DataFrame, Dataset}

class Distinct(override val uid: String)
  extends Transformer with DefaultParamsWritable {

  def this() = {
    this(Identifiable.randomUID("Distinct"))
  }

  final val colsParam = new StringArrayParam(this, "columns", "The dedup cols")
  setDefault(colsParam -> Array())

  override def transform(ds: Dataset[_]): DataFrame = {
    $(colsParam) match {
      case Array() => ds.distinct.toDF
      case arr => ds.dropDuplicates(arr).toDF
    }
  }

  override def transformSchema(schema: StructType): StructType = {
    schema
  }

  override def copy(extra: ParamMap): this.type = {
    defaultCopy(extra)
  }
}

object Distinct extends DefaultParamsReadable[Distinct] {
  def apply(): Distinct = {
    new Distinct()
  }

  def apply(cols: Array[String]): Distinct = {
    val distinct = new Distinct()
    distinct.set(distinct.colsParam -> cols)
  }

  def apply(cols: String): Distinct = {
    val distinct = new Distinct()
    distinct.set(distinct.colsParam -> Array(cols))
  }
}