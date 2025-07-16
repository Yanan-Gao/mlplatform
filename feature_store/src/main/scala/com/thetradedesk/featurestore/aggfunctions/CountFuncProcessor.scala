package com.thetradedesk.featurestore.aggfunctions

import com.thetradedesk.featurestore.aggfunctions.AggFunctions.AggFuncV2
import com.thetradedesk.featurestore.configs.FieldAggSpec
import org.apache.spark.sql._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.{DoubleType, LongType}


class CountFuncProcessor(func: AggFuncV2, fieldAggSpec: FieldAggSpec) extends AbstractAggFuncProcessor(func, fieldAggSpec) {
  override def aggCol(): Column = {
    count(col(fieldAggSpec.field)).cast(LongType)
  }

  override def merge(): Column = {
    sum(col(getMergeableColName)).cast(LongType)
  }
}

class NonZeroCountFuncProcessor(func: AggFuncV2, fieldAggSpec: FieldAggSpec) extends AbstractAggFuncProcessor(func, fieldAggSpec) {
  override def aggCol(): Column = {
    val input = col(fieldAggSpec.field)
    count(when(input =!= 0, input)).cast(LongType)
  }

  override def merge(): Column = {
    sum(col(getMergeableColName)).cast(LongType)
  }
}

class ArrayCountFuncProcessor(func: AggFuncV2, fieldAggSpec: FieldAggSpec) extends AbstractAggFuncProcessor(func, fieldAggSpec) {
  override def aggCol(): Column = {
    val input = col(fieldAggSpec.field)
    sum(when(input.isNotNull, size(input)).otherwise(null))
  }

  override def merge(): Column = {
    sum(col(getMergeableColName)).cast(LongType)
  }
}

class ArrayNonZeroCountFuncProcessor(func: AggFuncV2, fieldAggSpec: FieldAggSpec) extends AbstractAggFuncProcessor(func, fieldAggSpec) {
  override def aggCol(): Column = {
    val input = col(fieldAggSpec.field)
    sum(when(input.isNotNull, size(filter(input, element => element =!= 0))).otherwise(null))
  }

  override def merge(): Column = {
    sum(col(getMergeableColName)).cast(LongType)
  }
}

class VectorCountFuncProcessor(func: AggFuncV2, fieldAggSpec: FieldAggSpec) extends AbstractAggFuncProcessor(func, fieldAggSpec) {
  override def aggCol(): Column = {
    count(col(fieldAggSpec.field)).cast(LongType)
  }

  override def merge(): Column = {
    val input = col(getMergeableColName)
    sum(when(input.isNotNull, input)).cast(DoubleType)
  }
}
