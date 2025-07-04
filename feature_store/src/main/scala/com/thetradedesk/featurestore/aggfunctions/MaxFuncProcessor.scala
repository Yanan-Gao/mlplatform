package com.thetradedesk.featurestore.aggfunctions

import com.thetradedesk.featurestore.configs.FieldAggSpec
import com.thetradedesk.featurestore.features.Features.AggFunc
import com.thetradedesk.featurestore.features.Features.AggFunc.AggFunc
import org.apache.spark.sql._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.DoubleType
import com.thetradedesk.featurestore.transform.DescriptionAgg.VectorMaxAggregator

class MaxFuncProcessor extends ScalarAggFuncProcessor {
  override val aggFunc: AggFunc = AggFunc.Max
  override def aggCol(fieldAggSpec: FieldAggSpec): Column = {
    max(col(fieldAggSpec.field)).cast(DoubleType)
  }

  override def merge(fieldAggSpec: FieldAggSpec): Column = {
    max(col(s"${fieldAggSpec.field}_Max")).cast(DoubleType)
  }
}

class ArrayMaxFuncProcessor extends ArrayAggFuncProcessor {
  override val aggFunc: AggFunc = AggFunc.Max

  override def aggCol(fieldAggSpec: FieldAggSpec): Column = {
    val input = col(fieldAggSpec.field)
    max(when(input.isNotNull, array_max(input)).otherwise(null))
  }

  override def merge(fieldAggSpec: FieldAggSpec): Column = {
    max(col(s"${fieldAggSpec.field}_Max")).cast(DoubleType)
  }
}

class VectorMaxFuncProcessor extends VectorAggFuncProcessor {
  override val aggFunc: AggFunc = AggFunc.Max

  override def aggCol(fieldAggSpec: FieldAggSpec): Column = {
    val input = col(fieldAggSpec.field)
    val doubleArray = transform(input, num => num.cast(DoubleType))
    val descUdaf = udaf(new VectorMaxAggregator())
    descUdaf(doubleArray)
  }

  override def merge(fieldAggSpec: FieldAggSpec): Column = {
    val input = col(s"${fieldAggSpec.field}_Max")
    val doubleArray = transform(input, num => num.cast(DoubleType))
    val descUdaf = udaf(new VectorMaxAggregator())
    descUdaf(doubleArray)
  }
}
