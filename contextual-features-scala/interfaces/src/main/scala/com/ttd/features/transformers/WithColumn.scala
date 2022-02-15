package com.ttd.features.transformers

import org.apache.spark.ml.Transformer
import org.apache.spark.ml.param.{Param, ParamMap}
import org.apache.spark.ml.util.{DefaultParamsReadable, DefaultParamsWritable, Identifiable}
import org.apache.spark.sql.functions.expr
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql._

import scala.collection.JavaConverters._

class WithColumn(override val uid: String)
  extends Transformer with DefaultParamsWritable {

  def this() = {
    this(Identifiable.randomUID("Select"))
  }

  val colName = new Param[String](this, "colName", "Column name")
  val colExpr = new Param[String](this, "colExpr", "Column expression")

  override def transform(ds: Dataset[_]): DataFrame = {
    val colExpression = expr($(colExpr))
    ds.withColumn($(colName), colExpression).toDF()
  }

  override def transformSchema(schema: StructType): StructType = {
    transform(SparkSession.active.createDataFrame(List[Row]().asJava, schema)).schema
  }

  override def copy(extra: ParamMap): this.type = {
    defaultCopy(extra)
  }
}

object WithColumn extends DefaultParamsReadable[WithColumn] {
  def apply(colName: String, column: Column): WithColumn = {
    val wc = new WithColumn()
    wc.set(wc.colName -> colName)
    wc.set(wc.colExpr -> column.expr.sql)
  }
}