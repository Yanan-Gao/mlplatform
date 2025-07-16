package com.thetradedesk.featurestore.aggfunctions

import com.thetradedesk.featurestore.aggfunctions.AggFunctions.AggFuncV2
import com.thetradedesk.featurestore.configs.FieldAggSpec
import com.thetradedesk.featurestore.rsm.CommonEnums.Grain
import com.thetradedesk.featurestore.rsm.CommonEnums.Grain.Grain
import org.apache.spark.sql.Column
import org.apache.spark.sql.functions.col

trait AggFuncProcessor {

  // aggregate raw data and return a mergeable column
  def aggCol(): Column

  // merge mergeable
  def merge(): Column

  def export(window: Int = 0, grain: Option[Grain]): Array[Column]
}

abstract class AbstractAggFuncProcessor(aggFunc: AggFuncV2, fieldAggSpec: FieldAggSpec) extends AggFuncProcessor with Serializable {

  override def export(window: Int = 0, grain: Option[Grain]): Array[Column] =
    Array(col(getMergeableColName).alias(getExportColName(window, grain)))

  def getExportColName(window: Int = 0, grain: Option[Grain], func: AggFuncV2 = aggFunc): String = {
    val windowSuffix = getWindowSuffix(window, grain)
    s"${fieldAggSpec.field}_${func}_${windowSuffix}"
  }

  def getMergeableColName: String = {
    AggFuncProcessorUtils.getColNameByFunc(aggFunc, fieldAggSpec)
  }

  def getMergeableColName(func: AggFuncV2): String = {
    AggFuncProcessorUtils.getColNameByFunc(func, fieldAggSpec)
  }

  def getWindowSuffix(window: Int, grain: Option[Grain]): String = {
    val grainSuffix = grain match {
      case Some(Grain.Daily) => "D"
      case Some(Grain.Hourly) => "H"
      case _ => ""
    }
    s"Last${window}${grainSuffix}"
  }
}

object AggFuncProcessorUtils {

  def getColNameByFunc(func: AggFuncV2, fieldAggSpec: FieldAggSpec): String = {
    s"${fieldAggSpec.field}_${func}"
  }
}

/**
 * Factory for creating appropriate aggregation processors
 */
object AggFuncProcessorFactory {

  /**
   * Gets the appropriate processor for the given data type and function
   */
  def getProcessor(func: AggFuncV2, fieldAggSpec: FieldAggSpec): AggFuncProcessor = {
    if (fieldAggSpec.dataType.startsWith("array")) {
      func match {
        case AggFuncV2.Frequency(_) => new ArrayFrequencyProcessor(func.asInstanceOf[AggFuncV2.Frequency], fieldAggSpec)
        case AggFuncV2.Count => new ArrayCountFuncProcessor(func, fieldAggSpec)
        case AggFuncV2.NonZeroCount => new ArrayNonZeroCountFuncProcessor(func, fieldAggSpec)
        case AggFuncV2.Sum => new ArraySumFuncProcessor(func, fieldAggSpec)
        case AggFuncV2.Min => new ArrayMinFuncProcessor(func, fieldAggSpec)
        case AggFuncV2.Max => new ArrayMaxFuncProcessor(func, fieldAggSpec)
        case AggFuncV2.Mean => new ArrayMeanFuncProcessor(func, fieldAggSpec)
      }
    } else if (fieldAggSpec.dataType == "vector") {
      func match {
        case AggFuncV2.Frequency(_) => throw new UnsupportedOperationException("Frequency is not supported for vector data type")
        case AggFuncV2.Count => new VectorCountFuncProcessor(func, fieldAggSpec)
        case AggFuncV2.Sum => new VectorSumFuncProcessor(func, fieldAggSpec)
        case AggFuncV2.Min => new VectorMinFuncProcessor(func, fieldAggSpec)
        case AggFuncV2.Max => new VectorMaxFuncProcessor(func, fieldAggSpec)
        case AggFuncV2.Mean => new VectorMeanFuncProcessor(func, fieldAggSpec)
      }
    } else {
      func match {
        case AggFuncV2.Frequency(_) => new ScalarFrequencyProcessor(func.asInstanceOf[AggFuncV2.Frequency], fieldAggSpec)
        case AggFuncV2.Count => new CountFuncProcessor(func, fieldAggSpec)
        case AggFuncV2.Sum => new SumFuncProcessor(func, fieldAggSpec)
        case AggFuncV2.Min => new MinFuncProcessor(func, fieldAggSpec)
        case AggFuncV2.Max => new MaxFuncProcessor(func, fieldAggSpec)
        case AggFuncV2.Mean => new MeanFuncProcessor(func, fieldAggSpec)
        case AggFuncV2.NonZeroCount => new NonZeroCountFuncProcessor(func, fieldAggSpec)
        case AggFuncV2.NonZeroMean => new NonZeroMeanFuncProcessor(func, fieldAggSpec)
        case AggFuncV2.Percentile(_) => new PercentileFuncProcessor(func.asInstanceOf[AggFuncV2.Percentile], fieldAggSpec)
        case AggFuncV2.QuantileSummary => new QuantileSummaryFuncProcessor(func, fieldAggSpec)
      }
    }
  }
}
