package com.ttd.features.transformers

import org.apache.spark.ml.Transformer
import org.apache.spark.ml.param.{IntParam, ParamMap}
import org.apache.spark.ml.util.{DefaultParamsReadable, DefaultParamsWritable, Identifiable}
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.{DataFrame, Dataset}

class Repartition(override val uid: String)
  extends Transformer with DefaultParamsWritable {

  def this() = {
    this(Identifiable.randomUID("Repartition"))
  }

  final val numPartitions = new IntParam(this, "numPartitions", "The number of partitions")

  override def transform(ds: Dataset[_]): DataFrame = {
    ds
      .repartition($(numPartitions))
      .toDF()
  }

  override def transformSchema(schema: StructType): StructType = {
    schema
  }

  override def copy(extra: ParamMap): this.type = {
    defaultCopy(extra)
  }
}

object Repartition extends DefaultParamsReadable[Repartition]{
  def apply(numPartitions: Int): Repartition = {
    val repartition = new Repartition()
    repartition.set(repartition.numPartitions -> numPartitions)
  }
}

