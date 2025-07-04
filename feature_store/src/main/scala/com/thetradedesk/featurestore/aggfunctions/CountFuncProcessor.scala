package com.thetradedesk.featurestore.aggfunctions

import com.thetradedesk.featurestore.configs.FieldAggSpec
import com.thetradedesk.featurestore.features.Features.AggFunc
import com.thetradedesk.featurestore.features.Features.AggFunc.AggFunc
import org.apache.spark.sql._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.LongType
import org.apache.spark.sql.types.DoubleType

class CountFuncProcessor extends ScalarAggFuncProcessor {
  
  override val aggFunc: AggFunc = AggFunc.Count
  override def aggCol(fieldAggSpec: FieldAggSpec): Column = {
    count(col(fieldAggSpec.field)).cast(LongType)
  }
  override def merge(fieldAggSpec: FieldAggSpec): Column = {
    sum(col(s"${fieldAggSpec.field}_Count")).cast(LongType)
  }
}

class NonZeroCountFuncProcessor extends ScalarAggFuncProcessor {
  override val aggFunc: AggFunc = AggFunc.NonZeroCount
  override def aggCol(fieldAggSpec: FieldAggSpec): Column = {
    val input = col(fieldAggSpec.field)
    count(when(input =!= 0, input)).cast(LongType)
  }

  override def merge(fieldAggSpec: FieldAggSpec): Column = {
    sum(col(s"${fieldAggSpec.field}_NonZeroCount")).cast(LongType)
  }
}

class ArrayCountFuncProcessor extends ArrayAggFuncProcessor {
  override val aggFunc: AggFunc = AggFunc.Count

  override def aggCol(fieldAggSpec: FieldAggSpec): Column = {
    val input = col(fieldAggSpec.field)
    sum(when(input.isNotNull, size(input)).otherwise(null))
  }

  override def merge(fieldAggSpec: FieldAggSpec): Column = {       
    sum(col(s"${fieldAggSpec.field}_Count")).cast(LongType)
  }
}

class ArrayNonZeroCountFuncProcessor extends ArrayCountFuncProcessor {
  override val aggFunc: AggFunc = AggFunc.NonZeroCount
  override def aggCol(fieldAggSpec: FieldAggSpec): Column = {
    val input = col(fieldAggSpec.field)
    sum(when(input.isNotNull, size(filter(input, element => element =!= 0))).otherwise(null))
  }

  override def merge(fieldAggSpec: FieldAggSpec): Column = {       
    sum(col(s"${fieldAggSpec.field}_NonZeroCount")).cast(LongType)
  }
}

class VectorCountFuncProcessor extends VectorAggFuncProcessor {
  override val aggFunc: AggFunc = AggFunc.Count

  override def aggCol(fieldAggSpec: FieldAggSpec): Column = {
    count(col(fieldAggSpec.field)).cast(LongType)
  }

  override def merge(fieldAggSpec: FieldAggSpec): Column = {
    val input = col(s"${fieldAggSpec.field}_Count")
    sum(when(input.isNotNull, input)).cast(DoubleType)
  }
}
