package com.ttd.features.transformers

import org.apache.spark.ml.Transformer
import org.apache.spark.ml.param.{Param, ParamMap}
import org.apache.spark.ml.util.{DefaultParamsReadable, DefaultParamsWritable, Identifiable}
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.{DataFrame, Dataset}

class DropGlobalTempTable(override val uid: String)
  extends Transformer with DefaultParamsWritable {

  def this() = {
    this(Identifiable.randomUID("DropTempTable"))
  }

  final val tempTableName = new Param[String](this, "tableName", "The temp table name")

  override def transform(ds: Dataset[_]): DataFrame = {
    ds.sparkSession.catalog.dropGlobalTempView($(tempTableName))
    ds.toDF
  }

  override def transformSchema(schema: StructType): StructType = {
    schema
  }

  override def copy(extra: ParamMap): this.type = {
    defaultCopy(extra)
  }
}

object DropGlobalTempTable extends DefaultParamsReadable[DropGlobalTempTable] {
  def apply(tempTableName: String): DropGlobalTempTable = {
    val dropTempTable = new DropGlobalTempTable()
    dropTempTable.set(dropTempTable.tempTableName -> tempTableName)
  }
}

