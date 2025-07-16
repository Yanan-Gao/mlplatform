package com.thetradedesk.featurestore.aggfunctions

import com.thetradedesk.featurestore.aggfunctions.AggFunctions.AggFuncV2
import com.thetradedesk.featurestore.configs.FieldAggSpec
import com.thetradedesk.featurestore.features.Features.hashFeature
import com.thetradedesk.featurestore.rsm.CommonEnums.Grain.Grain
import com.thetradedesk.featurestore.transform.FrequencyAgg.{ArrayFrequencyAggregator, FrequencyAggregator, FrequencyMergeAggregator}
import com.thetradedesk.geronimo.shared.ARRAY_STRING_FEATURE_TYPE
import org.apache.spark.sql.Column
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.StringType

abstract class FrequencyFuncBaseProcessor(aggFunc: AggFuncV2.Frequency, fieldAggSpec: FieldAggSpec) extends AbstractAggFuncProcessor(aggFunc, fieldAggSpec) {

  override def merge(): Column = {
    val aggUdaf = udaf(new FrequencyMergeAggregator(aggFunc.topNs(0)))
    aggUdaf(col(getMergeableColName))
  }

  override def export(window: Int, grain: Option[Grain]): Array[Column] = {
    val sourceColumn = col(getMergeableColName)
    // Get all requested topNs, filter out invalids, and sort descending for convenience
    val topNs = aggFunc.topNs.distinct.filter(x => x > 0).sorted

    // UDF returns a Map from n to the top-n keys
    val topNMultiUdf = udf { map: Map[String, Int] =>
      if (map == null || map.isEmpty || topNs.isEmpty) {
        None
      } else {
        val sortedKeys = map.toSeq.sortBy(-_._2).map(_._1)
        // For each n, take the top n keys
        Some(topNs.map(n => n -> sortedKeys.take(n)).toMap)
      }
    }

    // The UDF returns Option[Map[Int, Seq[String]]], one entry for each n in topNs
    val baseCol = topNMultiUdf(sourceColumn)

    // For each n, extract the corresponding top-n list and alias appropriately
    val suffix = getWindowSuffix(window, grain)
    val topNCols = topNs.map { n =>
      if (fieldAggSpec.cardinality.isEmpty) {
        baseCol.getItem(n).alias(s"${fieldAggSpec.field}_Top${n}_$suffix")
      } else {
        hashFeature(baseCol.getItem(n), ARRAY_STRING_FEATURE_TYPE, fieldAggSpec.cardinality.get).alias(s"${fieldAggSpec.field}_Top${n}_${suffix}_Hash")
      }
    }

    if (aggFunc.topNs.contains(-1)) {
      topNCols :+ sourceColumn.alias(s"${fieldAggSpec.field}_Frequency_$suffix")
    } else {
      topNCols
    }
  }

}

class ScalarFrequencyProcessor(aggFunc: AggFuncV2.Frequency, fieldAggSpec: FieldAggSpec) extends FrequencyFuncBaseProcessor(aggFunc, fieldAggSpec) {

  override def aggCol(): Column = {
    val strCol = col(fieldAggSpec.field).cast(StringType)
    val topN = aggFunc.topNs(0)
    val aggUdaf = udaf(new FrequencyAggregator(topN))
    aggUdaf(strCol)
  }
}

class ArrayFrequencyProcessor(aggFunc: AggFuncV2.Frequency, fieldAggSpec: FieldAggSpec) extends FrequencyFuncBaseProcessor(aggFunc, fieldAggSpec) {
  override def aggCol(): Column = {
    val strCol = transform(col(fieldAggSpec.field), num => num.cast("string"))
    val topN = aggFunc.topNs(0)
    val aggUdaf = udaf(new ArrayFrequencyAggregator(topN))
    aggUdaf(strCol)
  }
}