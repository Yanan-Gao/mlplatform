package com.thetradedesk.featurestore.aggfunctions

import com.thetradedesk.featurestore.aggfunctions.AggFunctions.AggFuncV2
import com.thetradedesk.featurestore.aggfunctions.AggFunctions.AggFuncV2.{Count, NonZeroCount, Sum}
import com.thetradedesk.featurestore.configs.FieldAggSpec
import com.thetradedesk.featurestore.rsm.CommonEnums
import org.apache.spark.sql._
import org.apache.spark.sql.functions._

abstract class MeanFuncProcessorBase(func: AggFuncV2, field: FieldAggSpec) extends AbstractAggFuncProcessor(func, field) {
  override def aggCol(): Column = {
    throw new UnsupportedOperationException("Mean function is not supported for aggregation")
  }

  override def merge(): Column = {
    throw new UnsupportedOperationException("Mean function is not supported for merge")
  }
}

class MeanFuncProcessor(func: AggFuncV2, fieldAggSpec: FieldAggSpec) extends MeanFuncProcessorBase(func, fieldAggSpec) {
  override def export(window: Int, grain: Option[CommonEnums.Grain.Grain]): Array[Column] = {
    val countCol = col(getMergeableColName(Count))
    val sumCol = col(getMergeableColName(Sum))
    val meanCol = sumCol.cast("double") / countCol.cast("double")
    Array(meanCol.alias(getExportColName(window, grain)))
  }
}

class NonZeroMeanFuncProcessor(func: AggFuncV2, fieldAggSpec: FieldAggSpec) extends MeanFuncProcessorBase(func, fieldAggSpec) {
  override def export(window: Int, grain: Option[CommonEnums.Grain.Grain]): Array[Column] = {
    val countCol = col(getMergeableColName(NonZeroCount))
    val sumCol = col(getMergeableColName(Sum))
    val meanCol = sumCol.cast("double") / countCol.cast("double")
    Array(meanCol.alias(getExportColName(window, grain)))
  }
}

class ArrayMeanFuncProcessor(func: AggFuncV2, fieldAggSpec: FieldAggSpec) extends MeanFuncProcessorBase(func, fieldAggSpec) {
  override def export(window: Int, grain: Option[CommonEnums.Grain.Grain]): Array[Column] = {
    val countCol = col(getMergeableColName(Count))
    val sumCol = col(getMergeableColName(Sum))
    val meanCol = sumCol.cast("double") / countCol.cast("double")
    Array(meanCol.alias(getExportColName(window, grain)))
  }
}

class VectorMeanFuncProcessor(func: AggFuncV2, fieldAggSpec: FieldAggSpec) extends MeanFuncProcessorBase(func, fieldAggSpec) {
  override def export(window: Int, grain: Option[CommonEnums.Grain.Grain]): Array[Column] = {
    val countCol = col(getMergeableColName(Count))
    val sumCol = col(getMergeableColName(Sum))

    val meanUdf = udf[Option[Seq[Double]], Seq[Double], Long]((sumArray: Seq[Double], count: Long) => {
      if (sumArray == null || count == 0) {
        None
      } else {
        Some(sumArray.map(_ / count.toDouble))
      }
    })
    val meanCol = meanUdf(sumCol, countCol)

    Array(meanCol.alias(getExportColName(window, grain)))
  }
}