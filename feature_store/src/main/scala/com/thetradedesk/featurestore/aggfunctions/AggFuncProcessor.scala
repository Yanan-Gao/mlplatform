package com.thetradedesk.featurestore.aggfunctions

import com.thetradedesk.featurestore.configs.FieldAggSpec
import com.thetradedesk.featurestore.features.Features.AggFunc.AggFunc
import com.thetradedesk.featurestore.rsm.CommonEnums.Grain.Grain
import org.apache.spark.sql.Column
import org.apache.spark.sql.functions.col
import com.thetradedesk.featurestore.rsm.CommonEnums.Grain

trait AggFuncProcessor {

  val aggFunc: AggFunc

  def canProcess(dataType: String, func: AggFunc): Boolean

  def aggCol(fieldAggSpec: FieldAggSpec): Column

  def merge(fieldAggSpec: FieldAggSpec): Column

  def export(fieldAggSpec: FieldAggSpec, window: Int = 0, grain: Option[Grain]): Column = 
    col(s"${fieldAggSpec.field}_${aggFunc}").alias(getExportColName(fieldAggSpec, window, grain))

  def initialAggFuncs: Set[AggFunc] = Set(aggFunc)

  def getExportColName(fieldAggSpec: FieldAggSpec, window: Int = 0, grain: Option[Grain]): String = {
    val windowSuffix = grain match {
      case Some(Grain.Daily) => "D"
      case Some(Grain.Hourly) => "H"
      case _ => ""
    }
    s"${fieldAggSpec.field}_${aggFunc}_Last${window}${windowSuffix}"
  }
}

trait ScalarAggFuncProcessor extends AggFuncProcessor {
  override def canProcess(dataType: String, func: AggFunc): Boolean = 
    func == aggFunc && dataType != "vector" && !dataType.contains("array")
}

trait ArrayAggFuncProcessor extends AggFuncProcessor {
  override def canProcess(dataType: String, func: AggFunc): Boolean = 
    func == aggFunc && dataType.startsWith("array")
}

trait VectorAggFuncProcessor extends AggFuncProcessor {
  override def canProcess(dataType: String, func: AggFunc): Boolean = 
    func == aggFunc && dataType == "vector"
}

/**
 * Factory for creating appropriate aggregation processors
 */
object AggFuncProcessorFactory {

  private val processors: Seq[AggFuncProcessor] = Seq(
    // Frequency function processors
    new FrequencyProcessor,
    new ArrayFrequencyProcessor,
    new TopNProcessor,
    new ArrayTopNProcessor, 

    // Count function processors
    new CountFuncProcessor,
    new NonZeroCountFuncProcessor,
    new ArrayNonZeroCountFuncProcessor,
    new ArrayCountFuncProcessor,
    new VectorCountFuncProcessor,

    // Sum function processors
    new SumFuncProcessor,
    new ArraySumFuncProcessor,
    new VectorSumFuncProcessor,

    // Min function processors
    new MinFuncProcessor,
    new ArrayMinFuncProcessor,
    new VectorMinFuncProcessor,

    // Max function processors
    new MaxFuncProcessor,
    new ArrayMaxFuncProcessor,
    new VectorMaxFuncProcessor,

    // Mean function processors
    new MeanFuncProcessor,
    new ArrayMeanFuncProcessor,
    new VectorMeanFuncProcessor
  )

  /**
   * Gets the appropriate processor for the given data type and function
   */
  def getProcessor(dataType: String, func: AggFunc): AggFuncProcessor = {
    processors.find(_.canProcess(dataType, func))
      .getOrElse(throw new RuntimeException(s"No processor found for dataType: $dataType, func: $func, or it's not registered in AggFuncProcessorFactory"))
  }
}
