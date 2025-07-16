package com.thetradedesk.featurestore.aggfunctions

import com.thetradedesk.featurestore.aggfunctions.AggFunctions.AggFuncV2
import com.thetradedesk.featurestore.configs.FieldAggSpec
import com.thetradedesk.featurestore.transform.DescriptionAgg.VectorMaxAggregator
import org.apache.spark.sql._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.DoubleType

class MaxFuncProcessor(func: AggFuncV2, fieldAggSpec: FieldAggSpec) extends AbstractAggFuncProcessor(func, fieldAggSpec) {
  override def aggCol(): Column = {
    max(col(fieldAggSpec.field)).cast(DoubleType)
  }

  override def merge(): Column = {
    max(col(getMergeableColName)).cast(DoubleType)
  }
}

class ArrayMaxFuncProcessor(func: AggFuncV2, fieldAggSpec: FieldAggSpec) extends AbstractAggFuncProcessor(func, fieldAggSpec) {
  override def aggCol(): Column = {
    val input = col(fieldAggSpec.field)
    max(when(input.isNotNull, array_max(input)).otherwise(null))
  }

  override def merge(): Column = {
    max(col(getMergeableColName)).cast(DoubleType)
  }
}

class VectorMaxFuncProcessor(func: AggFuncV2, fieldAggSpec: FieldAggSpec) extends AbstractAggFuncProcessor(func, fieldAggSpec) {
  override def aggCol(): Column = {
    val input = col(fieldAggSpec.field)
    val doubleArray = transform(input, num => num.cast(DoubleType))
    val descUdaf = udaf(new VectorMaxAggregator())
    descUdaf(doubleArray)
  }

  override def merge(): Column = {
    val input = col(getMergeableColName)
    val doubleArray = transform(input, num => num.cast(DoubleType))
    val descUdaf = udaf(new VectorMaxAggregator())
    descUdaf(doubleArray)
  }
}
