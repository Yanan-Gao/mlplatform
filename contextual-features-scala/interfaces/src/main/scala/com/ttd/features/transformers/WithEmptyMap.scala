package com.ttd.features.transformers

import org.apache.spark.ml.Transformer
import org.apache.spark.ml.param.{Param, ParamMap}
import org.apache.spark.ml.util.{DefaultParamsReadable, DefaultParamsWritable, Identifiable}
import org.apache.spark.sql._
import org.apache.spark.sql.functions.map
import org.apache.spark.sql.types.{DataType, StructType}

import scala.collection.JavaConverters._

class WithEmptyMap(override val uid: String)
  extends Transformer with DefaultParamsWritable {

  def this() = {
    this(Identifiable.randomUID("Select"))
  }

  val colName = new Param[String](this, "colName", "Column name")
  val dataType = new Param[String](this, "dataType", "Map type")

  override def transform(ds: Dataset[_]): DataFrame = {
    ds.withColumn($(colName), map().cast(DataType.fromJson($(dataType)))).toDF()
  }

  override def transformSchema(schema: StructType): StructType = {
    transform(SparkSession.active.createDataFrame(List[Row]().asJava, schema)).schema
  }

  override def copy(extra: ParamMap): this.type = {
    defaultCopy(extra)
  }
}

object WithEmptyMap extends DefaultParamsReadable[WithEmptyMap] {
  def apply(colName: String, dataType: DataType): WithEmptyMap = {
    val wc = new WithEmptyMap()
    wc.set(wc.colName -> colName)
    wc.set(wc.dataType -> dataType.json)
  }
}
