package com.thetradedesk.featurestore.aggfunctions

import com.thetradedesk.featurestore.aggfunctions.AggFunctions.AggFuncV2
import com.thetradedesk.featurestore.configs.FieldAggSpec
import com.thetradedesk.featurestore.transform.DescriptionAgg.VectorMinAggregator
import org.apache.spark.sql._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.DoubleType

class MinFuncProcessor(func: AggFuncV2, fieldAggSpec: FieldAggSpec) extends AbstractAggFuncProcessor(func, fieldAggSpec) {
  override def aggCol(): Column = {
    min(col(fieldAggSpec.field)).cast(DoubleType)
  }

  override def merge(): Column = {
    min(col(getMergeableColName)).cast(DoubleType)
  }
}

class ArrayMinFuncProcessor(func: AggFuncV2, fieldAggSpec: FieldAggSpec) extends AbstractAggFuncProcessor(func, fieldAggSpec) {
  override def aggCol(): Column = {
    val input = col(fieldAggSpec.field)
    min(when(input.isNotNull, array_min(input)).otherwise(null))
  }

  override def merge(): Column = {
    min(col(getMergeableColName)).cast(DoubleType)
  }
}

class VectorMinFuncProcessor(func: AggFuncV2, fieldAggSpec: FieldAggSpec) extends AbstractAggFuncProcessor(func, fieldAggSpec) {
  override def aggCol(): Column = {
    val input = col(fieldAggSpec.field)
    val doubleArray = transform(input, num => num.cast(DoubleType))
    val descUdaf = udaf(new VectorMinAggregator())
    descUdaf(doubleArray)
  }

  override def merge(): Column = {
    val input = col(getMergeableColName)
    val doubleArray = transform(input, num => num.cast(DoubleType))
    val descUdaf = udaf(new VectorMinAggregator())
    descUdaf(doubleArray)
  }
}
