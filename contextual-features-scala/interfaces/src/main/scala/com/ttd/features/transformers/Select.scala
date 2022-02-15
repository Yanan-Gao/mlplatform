package com.ttd.features.transformers

import org.apache.spark.ml.Transformer
import org.apache.spark.ml.param.{ParamMap, StringArrayParam}
import org.apache.spark.ml.util.{DefaultParamsReadable, DefaultParamsWritable, Identifiable}
import org.apache.spark.sql._
import org.apache.spark.sql.functions.expr
import org.apache.spark.sql.types.StructType

import scala.collection.JavaConverters._

class Select(override val uid: String)
  extends Transformer with DefaultParamsWritable {

  def this() = {
    this(Identifiable.randomUID("Select"))
  }

  final val colsParam = new StringArrayParam(this, "colsParam", "The select columns")

  override def transform(ds: Dataset[_]): DataFrame = {
    val colExpressions = $(colsParam).map(expr)
    ds.select(colExpressions:_*)
  }

  override def transformSchema(schema: StructType): StructType = {
    transform(SparkSession.active.createDataFrame(List[Row]().asJava, schema)).schema
  }

  override def copy(extra: ParamMap): this.type = {
    defaultCopy(extra)
  }
}

object Select extends DefaultParamsReadable[Select] {
  def apply(cols: Seq[Column]): Select = {
    val agg = new Select()
    agg.set(agg.colsParam -> cols.map(_.expr.sql).toArray)
  }

  def apply(col: String, rest: String*): Select = {
    val agg = new Select()
    agg.set(agg.colsParam -> (col +: rest).toArray)
  }
}
