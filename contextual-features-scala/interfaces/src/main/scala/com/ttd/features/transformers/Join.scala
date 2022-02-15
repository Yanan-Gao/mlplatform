package com.ttd.features.transformers

import org.apache.spark.ml.Transformer
import org.apache.spark.ml.param.{Param, ParamMap, StringArrayParam}
import org.apache.spark.ml.util.{DefaultParamsReadable, DefaultParamsWritable, Identifiable}
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.{DataFrame, Dataset, Row, SparkSession}

import scala.collection.JavaConverters._

/**
 * Joins the dataframe being transformed with the dataframe provided in constructor, given join columns and join type.
 * Supported join methods: inner, cross, outer, full, full_outer, left, left_outer,
 * right, right_outer, left_semi and left_anti.
 */
class Join(override val uid: String)
  extends Transformer with DefaultParamsWritable {

  def this() = {
    this(Identifiable.randomUID("Join"))
  }

  final val rightTempName = new Param[String](this, "rightTempName", "The right table temp name")
  final val joinType = new Param[String](this, "joinType", "The join type")
  final val joinCols = new StringArrayParam(this, "joinCols", "The join columns")

  private def _transform(left: Dataset[_], right: Dataset[_]): DataFrame = {
    left.join(right, $(joinCols), $(joinType))
  }

  override def transform(ds: Dataset[_]): DataFrame = {
    val right = ds.sparkSession.table("global_temp." + $(rightTempName))
    _transform(ds, right)
  }

  override def transformSchema(schema: StructType): StructType = {
    val l = SparkSession.active.createDataFrame(List[Row]().asJava, schema)
    val r = SparkSession.active.createDataFrame(List[Row]().asJava,
      SparkSession.active.table("global_temp." + $(rightTempName)).schema)
    _transform(l, r).schema
  }

  override def copy(extra: ParamMap): this.type = {
    defaultCopy(extra)
  }
}

object Join extends DefaultParamsReadable[Join] {
  def apply(rightTempName: String, cols: Array[String], joinType: String = "inner"): Join = {
    val join = new Join()
    join.set(join.rightTempName -> rightTempName)
    join.set(join.joinCols -> cols)
    join.set(join.joinType -> joinType)
  }

  def apply(rightTempName: String, col: String, joinType: String): Join = {
    val join = new Join()
    join.set(join.rightTempName -> rightTempName)
    join.set(join.joinCols -> Array(col))
    join.set(join.joinType -> joinType)
  }
}