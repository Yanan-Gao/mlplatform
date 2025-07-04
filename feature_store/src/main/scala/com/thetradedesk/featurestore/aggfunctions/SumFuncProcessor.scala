package com.thetradedesk.featurestore.aggfunctions

import com.thetradedesk.featurestore.configs.FieldAggSpec
import com.thetradedesk.featurestore.features.Features.AggFunc
import com.thetradedesk.featurestore.features.Features.AggFunc.AggFunc
import org.apache.spark.sql._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.DoubleType
import com.thetradedesk.featurestore.transform.DescriptionAgg.VectorSumAggregator

class SumFuncProcessor extends ScalarAggFuncProcessor {
  override val aggFunc: AggFunc = AggFunc.Sum

  override def aggCol(fieldAggSpec: FieldAggSpec): Column = {
    sum(col(fieldAggSpec.field)).cast(DoubleType)
  }

  override def merge(fieldAggSpec: FieldAggSpec): Column = {
    sum(col(s"${fieldAggSpec.field}_Sum")).cast(DoubleType)
  }
}

class ArraySumFuncProcessor extends ArrayAggFuncProcessor {
  override val aggFunc: AggFunc = AggFunc.Sum

  override def aggCol(fieldAggSpec: FieldAggSpec): Column = {
    val input = col(fieldAggSpec.field)
    sum(when(input.isNotNull, aggregate(input, lit(0.0), (acc, x) => acc + x)).otherwise(null))
  }

  override def merge(fieldAggSpec: FieldAggSpec): Column = {
    sum(col(s"${fieldAggSpec.field}_Sum")).cast(DoubleType)
  }
}

class VectorSumFuncProcessor extends VectorAggFuncProcessor {
  override val aggFunc: AggFunc = AggFunc.Sum

  override def aggCol(fieldAggSpec: FieldAggSpec): Column = {
    val input = col(fieldAggSpec.field)
    val doubleArray = transform(input, num => num.cast(DoubleType))
    val descUdaf = udaf(new VectorSumAggregator())
    descUdaf(doubleArray)
  }

  override def merge(fieldAggSpec: FieldAggSpec): Column = {
    val input = col(s"${fieldAggSpec.field}_Sum")
    val doubleArray = transform(input, num => num.cast(DoubleType))
    val descUdaf = udaf(new VectorSumAggregator())
    descUdaf(doubleArray)
  }
}
