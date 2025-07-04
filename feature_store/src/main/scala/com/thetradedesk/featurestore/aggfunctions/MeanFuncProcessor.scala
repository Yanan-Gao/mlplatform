package com.thetradedesk.featurestore.aggfunctions

import com.thetradedesk.featurestore.configs.FieldAggSpec
import com.thetradedesk.featurestore.features.Features.AggFunc
import com.thetradedesk.featurestore.features.Features.AggFunc.AggFunc
import org.apache.spark.sql._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.DoubleType
import com.thetradedesk.featurestore.rsm.CommonEnums

trait MeanFuncProcessorBase extends AggFuncProcessor {
  override val aggFunc: AggFunc = AggFunc.Mean

  override def aggCol(fieldAggSpec: FieldAggSpec): Column = {
    throw new UnsupportedOperationException("Mean function is not supported for aggregation")
  }

  override def merge(fieldAggSpec: FieldAggSpec): Column = {
    throw new UnsupportedOperationException("Mean function is not supported for merge")
  }

  override def initialAggFuncs: Set[AggFunc] = Set(AggFunc.Sum, AggFunc.Count)
}

class MeanFuncProcessor extends ScalarAggFuncProcessor with MeanFuncProcessorBase {
  override def export(fieldAggSpec: FieldAggSpec, window: Int, grain: Option[CommonEnums.Grain.Grain]): Column = {
    val countCol = col(s"${fieldAggSpec.field}_Count")
    val sumCol = col(s"${fieldAggSpec.field}_Sum")
    val meanCol = sumCol.cast("double") / countCol.cast("double")
    meanCol.alias(getExportColName(fieldAggSpec, window, grain))
  }
}

class ArrayMeanFuncProcessor extends ArrayAggFuncProcessor with MeanFuncProcessorBase {
  override def export(fieldAggSpec: FieldAggSpec, window: Int, grain: Option[CommonEnums.Grain.Grain]): Column = {
    val countCol = col(s"${fieldAggSpec.field}_Count")
    val sumCol = col(s"${fieldAggSpec.field}_Sum")
    val meanCol = sumCol.cast("double") / countCol.cast("double")
    meanCol.alias(getExportColName(fieldAggSpec, window, grain))
  }
}

class VectorMeanFuncProcessor extends VectorAggFuncProcessor with MeanFuncProcessorBase {
  override def export(fieldAggSpec: FieldAggSpec, window: Int, grain: Option[CommonEnums.Grain.Grain]): Column = {
    val countCol = col(s"${fieldAggSpec.field}_Count")
    val sumCol = col(s"${fieldAggSpec.field}_Sum")
    
    val meanUdf = udf[Option[Seq[Double]], Seq[Double], Long]((sumArray: Seq[Double], count: Long) => {
        if (sumArray == null || count == 0) {
          None
        } else {
          Some(sumArray.map(_ / count.toDouble))
        }
      })
    val meanCol = meanUdf(sumCol, countCol)

    meanCol.alias(getExportColName(fieldAggSpec, window, grain))
  }
}