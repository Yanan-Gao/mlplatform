package com.thetradedesk.featurestore.aggfunctions

import com.thetradedesk.featurestore.configs.FieldAggSpec
import com.thetradedesk.featurestore.features.Features.AggFunc
import com.thetradedesk.featurestore.features.Features.AggFunc.AggFunc
import com.thetradedesk.featurestore.rsm.CommonEnums.Grain
import com.thetradedesk.featurestore.rsm.CommonEnums.Grain.Grain
import com.thetradedesk.featurestore.transform.FrequencyAgg.{ArrayFrequencyAggregator, FrequencyAggregator, FrequencyMergeAggregator}
import org.apache.spark.sql.Column
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.StringType


trait ScalarFrequencyBaseProcessor extends ScalarAggFuncProcessor {
  override val aggFunc: AggFunc = AggFunc.Frequency
  override def aggCol(fieldAggSpec: FieldAggSpec): Column = {
    val strCol = col(fieldAggSpec.field).cast(StringType)
    val aggUdaf = udaf(new FrequencyAggregator(fieldAggSpec.topN))
    aggUdaf(strCol)
  }

  override def merge(fieldAggSpec: FieldAggSpec): Column = {
    val aggUdaf = udaf(new FrequencyMergeAggregator(fieldAggSpec.topN))
    aggUdaf(col(s"${fieldAggSpec.field}_${aggFunc}"))
  }

}

trait ArrayFrequencyBaseProcessor extends ArrayAggFuncProcessor {
  override val aggFunc: AggFunc = AggFunc.Frequency
  override def aggCol(fieldAggSpec: FieldAggSpec): Column = {
    val strCol = transform(col(fieldAggSpec.field), num => num.cast("string"))
    val aggUdaf = udaf(new ArrayFrequencyAggregator(fieldAggSpec.topN))
    aggUdaf(strCol)
  }

  override def merge(fieldAggSpec: FieldAggSpec): Column = {
    val aggUdaf = udaf(new FrequencyMergeAggregator(fieldAggSpec.topN))
    aggUdaf(col(s"${fieldAggSpec.field}_${aggFunc}"))
  }

}

// process column with scalar type
class FrequencyProcessor extends ScalarFrequencyBaseProcessor

class ArrayFrequencyProcessor extends ArrayFrequencyBaseProcessor 

class TopNProcessor extends ScalarFrequencyBaseProcessor {
  override val aggFunc: AggFunc = AggFunc.TopN

  override def initialAggFuncs: Set[AggFunc] = Set(AggFunc.Frequency)

  override def export(fieldAggSpec: FieldAggSpec, window: Int, grain: Option[Grain]): Column = {
    val sourceColumn = col(s"${fieldAggSpec.field}_${AggFunc.Frequency}")
    TopNExport.export(fieldAggSpec, sourceColumn, window, grain)
  }
}

class ArrayTopNProcessor extends ArrayFrequencyBaseProcessor {
  override val aggFunc: AggFunc = AggFunc.TopN

  override def initialAggFuncs: Set[AggFunc] = Set(AggFunc.Frequency)

  override def export(fieldAggSpec: FieldAggSpec, window: Int, grain: Option[Grain]): Column = {
    val sourceColumn = col(s"${fieldAggSpec.field}_${AggFunc.Frequency}")
    TopNExport.export(fieldAggSpec, sourceColumn, window, grain)
  }
}

object TopNExport {
  def export(fieldAggSpec: FieldAggSpec, sourceColumn: Column, window: Int, grain: Option[Grain]): Column = {
    val suffix = grain.get match {
      case Grain.Daily => "D"
      case Grain.Hourly => "H"
      case _ => throw new IllegalArgumentException(s"Unsupported window unit: $grain")
    }
        val topNUdf = udf[Option[Seq[String]], Map[String, Int]]((map: Map[String, Int]) => {
      if (map == null || map.isEmpty) {
        None
      } else {
        Some(map.toSeq
          .sortBy(-_._2) // Sort by count in descending order
          .take(fieldAggSpec.topN)
          .map(_._1)) // Extract only the keys
      }
    })
    
    val exportColumnName = s"${fieldAggSpec.field}_Top${fieldAggSpec.topN}_$suffix"
    topNUdf(sourceColumn).alias(exportColumnName)
  }
}