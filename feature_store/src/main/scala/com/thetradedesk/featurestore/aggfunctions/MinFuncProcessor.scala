package com.thetradedesk.featurestore.aggfunctions

import com.thetradedesk.featurestore.configs.FieldAggSpec
import com.thetradedesk.featurestore.features.Features.AggFunc
import com.thetradedesk.featurestore.features.Features.AggFunc.AggFunc
import org.apache.spark.sql._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.DoubleType
import com.thetradedesk.featurestore.transform.DescriptionAgg.VectorMinAggregator

class MinFuncProcessor extends ScalarAggFuncProcessor {
  override val aggFunc: AggFunc = AggFunc.Min

  override def aggCol(fieldAggSpec: FieldAggSpec): Column = {
    min(col(fieldAggSpec.field)).cast(DoubleType)
  }

  override def merge(fieldAggSpec: FieldAggSpec): Column = {
    min(col(s"${fieldAggSpec.field}_Min")).cast(DoubleType)
  }
}

class ArrayMinFuncProcessor extends ArrayAggFuncProcessor {
  override val aggFunc: AggFunc = AggFunc.Min

  override def aggCol(fieldAggSpec: FieldAggSpec): Column = {
    val input = col(fieldAggSpec.field)
    min(when(input.isNotNull, array_min(input)).otherwise(null))
  }

  override def merge(fieldAggSpec: FieldAggSpec): Column = {
    min(col(s"${fieldAggSpec.field}_Min")).cast(DoubleType)
  }
}

class VectorMinFuncProcessor extends VectorAggFuncProcessor {
  override val aggFunc: AggFunc = AggFunc.Min

  override def aggCol(fieldAggSpec: FieldAggSpec): Column = {
    val input = col(fieldAggSpec.field)
    val doubleArray = transform(input, num => num.cast(DoubleType))
    val descUdaf = udaf(new VectorMinAggregator())
    descUdaf(doubleArray)
  }

  override def merge(fieldAggSpec: FieldAggSpec): Column = {
    val input = col(s"${fieldAggSpec.field}_Min")
    val doubleArray = transform(input, num => num.cast(DoubleType))
    val descUdaf = udaf(new VectorMinAggregator())
    descUdaf(doubleArray)
  }
}
