package com.thetradedesk.featurestore.aggfunctions

import com.thetradedesk.featurestore.aggfunctions.AggFunctions.AggFuncV2
import com.thetradedesk.featurestore.configs.FieldAggSpec
import com.thetradedesk.featurestore.transform.DescriptionAgg.VectorSumAggregator
import org.apache.spark.sql._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.DoubleType

class SumFuncProcessor(func: AggFuncV2, fieldAggSpec: FieldAggSpec) extends AbstractAggFuncProcessor(func, fieldAggSpec) {
  override def aggCol(): Column = {
    sum(col(fieldAggSpec.field)).cast(DoubleType)
  }

  override def merge(): Column = {
    sum(col(getMergeableColName)).cast(DoubleType)
  }
}

class ArraySumFuncProcessor(func: AggFuncV2, fieldAggSpec: FieldAggSpec) extends AbstractAggFuncProcessor(func, fieldAggSpec) {
  override def aggCol(): Column = {
    val input = col(fieldAggSpec.field)
    sum(when(input.isNotNull, aggregate(input, lit(0.0), (acc, x) => acc + x)).otherwise(null))
  }

  override def merge(): Column = {
    sum(col(getMergeableColName)).cast(DoubleType)
  }
}

class VectorSumFuncProcessor(func: AggFuncV2, fieldAggSpec: FieldAggSpec) extends AbstractAggFuncProcessor(func, fieldAggSpec) {
  override def aggCol(): Column = {
    val input = col(fieldAggSpec.field)
    val doubleArray = transform(input, num => num.cast(DoubleType))
    val descUdaf = udaf(new VectorSumAggregator())
    descUdaf(doubleArray)
  }

  override def merge(): Column = {
    val input = col(s"${fieldAggSpec.field}_Sum")
    val doubleArray = transform(input, num => num.cast(DoubleType))
    val descUdaf = udaf(new VectorSumAggregator())
    descUdaf(doubleArray)
  }
}
