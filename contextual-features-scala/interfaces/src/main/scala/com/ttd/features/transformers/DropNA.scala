package com.ttd.features.transformers

import org.apache.spark.ml.Transformer
import org.apache.spark.ml.param.{Param, ParamMap, StringArrayParam}
import org.apache.spark.ml.util.{DefaultParamsReadable, DefaultParamsWritable, Identifiable}
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.{DataFrame, Dataset}

class DropNA(override val uid: String)
  extends Transformer with DefaultParamsWritable {

  def this() = {
    this(Identifiable.randomUID("DropNA"))
  }

  final val colsParam = new StringArrayParam(this, "columns", "The na cols")
  setDefault(colsParam -> Array())

  final val howParam = new Param[String](this, "how", "How to drop")
  setDefault(howParam -> "any")

  override def transform(ds: Dataset[_]): DataFrame = {
    $(colsParam) match {
      case Array() => ds.na.drop($(howParam))
      case arr => ds.na.drop($(howParam), arr)
    }
  }

  override def transformSchema(schema: StructType): StructType = {
    schema
  }

  override def copy(extra: ParamMap): this.type = {
    defaultCopy(extra)
  }
}

object DropNA extends DefaultParamsReadable[DropNA] {
  def apply(): DropNA = {
    new DropNA()
  }

  def apply(cols: Array[String]): DropNA = {
    val dropNA = new DropNA()
    dropNA.set(dropNA.colsParam -> cols)
  }

  def apply(cols: Array[String], how: String): DropNA = {
    val dropNA = new DropNA()
    dropNA.set(dropNA.colsParam -> cols)
    dropNA.set(dropNA.howParam -> how)
  }
}