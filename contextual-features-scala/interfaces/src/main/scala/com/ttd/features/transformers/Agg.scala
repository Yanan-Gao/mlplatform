package com.ttd.features.transformers

import org.apache.spark.ml.Transformer
import org.apache.spark.ml.param.{ParamMap, StringArrayParam}
import org.apache.spark.ml.util.{DefaultParamsReadable, DefaultParamsWritable, Identifiable}
import org.apache.spark.sql.functions.expr
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql._

import scala.collection.JavaConverters._

class Agg(override val uid: String)
  extends Transformer with DefaultParamsWritable {

  def this() = {
    this(Identifiable.randomUID("Agg"))
  }

  final val groupByParam = new StringArrayParam(this, "groupBy", "The group by cols")
  final val aggParam = new StringArrayParam(this, "agg", "The agg cols")

  override def transform(ds: Dataset[_]): DataFrame = {
    val groupExpressions = $(groupByParam).map(expr)
    val aggExpressions = $(aggParam).map(expr)

    ds
      .groupBy(groupExpressions:_*)
      .agg(aggExpressions.head, aggExpressions.tail:_*)
  }

  override def transformSchema(schema: StructType): StructType = {
    transform(SparkSession.active.createDataFrame(List[Row]().asJava, schema)).schema
  }

  override def copy(extra: ParamMap): this.type = {
    defaultCopy(extra)
  }
}

object Agg extends DefaultParamsReadable[Agg]{
  def apply(groupBy: Seq[Column], aggCols: Seq[Column]): Agg = {
    val agg = new Agg()
    agg.set(agg.groupByParam -> groupBy.map(_.expr.sql).toArray)
    agg.set(agg.aggParam -> aggCols.map(_.expr.sql).toArray)
  }
}

