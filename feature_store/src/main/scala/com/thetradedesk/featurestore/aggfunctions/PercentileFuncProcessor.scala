package com.thetradedesk.featurestore.aggfunctions

import com.tdunning.math.stats.MergingDigest
import com.thetradedesk.featurestore.aggfunctions.AggFunctions.AggFuncV2
import com.thetradedesk.featurestore.aggfunctions.AggFunctions.AggFuncV2.QuantileSummary
import com.thetradedesk.featurestore.configs.FieldAggSpec
import com.thetradedesk.featurestore.rsm.CommonEnums.Grain.Grain
import com.thetradedesk.featurestore.transform.{QuantileSummaryAgg, QuantileSummaryMergeAgg}
import org.apache.spark.sql.Column
import org.apache.spark.sql.functions.{col, udaf, udf}
import org.apache.spark.sql.types.DoubleType

import java.nio.ByteBuffer

// this is processor used for original functions, to export the actual percentile values from aggregated results
class PercentileFuncProcessor(func: AggFuncV2.Percentile, fieldAggSpec: FieldAggSpec) extends AbstractAggFuncProcessor(func, fieldAggSpec) {

  override def aggCol(): Column = {
    throw new UnsupportedOperationException("Percentile is not supported for aggregation")
  }

  override def merge(): Column = {
    throw new UnsupportedOperationException("Percentile is not supported for merge")
  }

  override def export(window: Int, grain: Option[Grain]): Array[Column] = {
    // extract as a map that has percentile as key and quantile as value
    val percentiles = func.percentiles.distinct.sorted
    val extractQuantilesUDF = udf(
      (bytes: Array[Byte]) => {
        if (bytes == null || bytes.isEmpty || percentiles.isEmpty) {
          None
        } else {
          val digest = MergingDigest.fromBytes(ByteBuffer.wrap(bytes))
          Some(percentiles.map(p => p -> digest.quantile(p.toDouble / 100)).toMap)
        }
      })
    val sourceColumn = col(getMergeableColName(QuantileSummary))
    val baseCol = extractQuantilesUDF(sourceColumn)
    val suffix = getWindowSuffix(window, grain)

    percentiles.map { p =>
      baseCol.getItem(p)
        .asInstanceOf[Column] // Spark SQL Column
        .alias(s"${fieldAggSpec.field}_P${p}_$suffix")
    }
  }
}

//// this is processor used for aggregate and merge process
class QuantileSummaryFuncProcessor(func: AggFuncV2, fieldAggSpec: FieldAggSpec) extends AbstractAggFuncProcessor(func, fieldAggSpec) {

  override def aggCol(): Column = {
    val doubleCol = col(fieldAggSpec.field).cast(DoubleType)
    val aggUdaf = udaf(new QuantileSummaryAgg())
    aggUdaf(doubleCol)
  }

  override def merge(): Column = {
    val aggUdaf = udaf(new QuantileSummaryMergeAgg())
    aggUdaf(col(getMergeableColName))
  }

  override def export(window: Int, grain: Option[Grain]): Array[Column] = {
    throw new UnsupportedOperationException("QuantileSummary is not supported for export")
  }

}
