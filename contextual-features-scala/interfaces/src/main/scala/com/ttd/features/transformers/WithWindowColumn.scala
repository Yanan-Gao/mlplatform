package com.ttd.features.transformers

import org.apache.spark.ml.Transformer
import org.apache.spark.ml.param.{Param, ParamMap, StringArrayParam}
import org.apache.spark.ml.util.{DefaultParamsReadable, DefaultParamsWritable, Identifiable}
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions.expr
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql._

import scala.collection.JavaConverters._

/**
 * Unfortunately Window functions don't serialize to sql string easily
 * This class wraps a window operation into a transformer stage that is serializable
 */
class WithWindowColumn(override val uid: String)
  extends Transformer with DefaultParamsWritable {

  def this() = {
    this(Identifiable.randomUID("Agg"))
  }

  final val colName = new Param[String](this, "colName", "Name of this column")
  final val column = new Param[String](this, "column", "The column expression")
  final val partitionCols = new StringArrayParam(this, "partitionCols", "The partition cols")
  final val orderBy = new StringArrayParam(this, "orderBy", "The orderBy cols")

  override def transform(ds: Dataset[_]): DataFrame = {
    val partitionExpressions = $(partitionCols).map(expr)
    val orderExpressions = $(orderBy).map(expr)
    val columnExpression = expr($(column))
    val windowExpression = Window.partitionBy(partitionExpressions:_*).orderBy(orderExpressions:_*)

    ds
      .withColumn($(colName), columnExpression.over(windowExpression))
  }

  override def transformSchema(schema: StructType): StructType = {
    transform(SparkSession.active.createDataFrame(List[Row]().asJava, schema)).schema
  }

  override def copy(extra: ParamMap): this.type = {
    defaultCopy(extra)
  }
}

object WithWindowColumn extends DefaultParamsReadable[WithWindowColumn] {
  def apply(colName: String, column: Column, partitionBy: Seq[Column], orderBy: Seq[Column]): WithWindowColumn = {
    val agg = new WithWindowColumn()
    agg.set(agg.colName -> colName)
    agg.set(agg.column -> column.expr.sql)
    agg.set(agg.partitionCols -> partitionBy.map(_.expr.sql).toArray)
    agg.set(agg.orderBy -> orderBy.map(_.expr.sql).toArray)
  }
}

