package com.ttd.features.transformers

import org.apache.spark.ml.Transformer
import org.apache.spark.ml.param.{Param, ParamMap}
import org.apache.spark.ml.util.{DefaultParamsReadable, DefaultParamsWritable, Identifiable}
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.{DataFrame, Dataset, SparkSession}

class ReadGlobalTempTable(override val uid: String)
  extends Transformer with DefaultParamsWritable {

  def this() = {
    this(Identifiable.randomUID("ReadTempTable"))
  }

  final val tempTableName = new Param[String](this, "tableName", "The temp table name")

  override def transform(ds: Dataset[_]): DataFrame = {
    ds.sparkSession.table("global_temp." + $(tempTableName))
  }

  override def transformSchema(schema: StructType): StructType = {
    SparkSession.active.table("global_temp." + $(tempTableName)).schema
  }

  override def copy(extra: ParamMap): this.type = {
    defaultCopy(extra)
  }
}

object ReadGlobalTempTable extends DefaultParamsReadable[ReadGlobalTempTable] {
  def apply(tempTableName: String): ReadGlobalTempTable = {
    val readTempTable = new ReadGlobalTempTable()
    readTempTable.set(readTempTable.tempTableName -> tempTableName)
  }
}


