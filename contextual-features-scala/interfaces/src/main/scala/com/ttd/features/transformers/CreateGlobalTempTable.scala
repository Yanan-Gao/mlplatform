package com.ttd.features.transformers

import org.apache.spark.ml.Transformer
import org.apache.spark.ml.param.{Param, ParamMap}
import org.apache.spark.ml.util.{DefaultParamsReadable, DefaultParamsWritable, Identifiable}
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.{DataFrame, Dataset, Row, SparkSession}

import scala.collection.JavaConverters._

class CreateGlobalTempTable(override val uid: String)
  extends Transformer with DefaultParamsWritable {

  def this() = {
    this(Identifiable.randomUID("CreateTempTable"))
  }

  final val tempTableName = new Param[String](this, "tableName", "The temp table name")

  override def transform(ds: Dataset[_]): DataFrame = {
    ds.createOrReplaceGlobalTempView($(tempTableName))
    ds.toDF
  }

  override def transformSchema(schema: StructType): StructType = {
    // create an empty temporary view of this schema so later transformSchema's can see it
    transform(SparkSession.active.createDataFrame(List[Row]().asJava, schema))
    schema
  }

  override def copy(extra: ParamMap): this.type = {
    defaultCopy(extra)
  }
}

object CreateGlobalTempTable extends DefaultParamsReadable[CreateGlobalTempTable] {
  def apply(tempTableName: String): CreateGlobalTempTable = {
    val createTempTable = new CreateGlobalTempTable()
    createTempTable.set(createTempTable.tempTableName -> tempTableName)
  }
}

