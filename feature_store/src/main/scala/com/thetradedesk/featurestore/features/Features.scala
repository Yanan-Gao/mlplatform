package com.thetradedesk.featurestore.features

import com.thetradedesk.featurestore.features.Features.AggFunc.AggFunc
import com.thetradedesk.geronimo.shared.{ARRAY_INT_FEATURE_TYPE, ARRAY_LONG_FEATURE_TYPE, ARRAY_STRING_FEATURE_TYPE, FLOAT_FEATURE_TYPE, INT_FEATURE_TYPE, LONG_FEATURE_TYPE, STRING_FEATURE_TYPE, shiftModArrayUdf, shiftModUdf}
import com.thetradedesk.spark.sql.SQLFunctions._
import io.circe.Decoder
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{Column, Dataset}


object Features {

  object AggFunc extends Enumeration {
    type AggFunc = Value
    val Sum, Count, Mean, NonZeroMean, Median, Percentiles, Desc, NonZeroCount, Min, Max, TopN, Frequency
    = Value

    def fromString(value: String): Option[AggFunc] = {
      values.find(_.toString.equalsIgnoreCase(value))
    }

    def getInitialAggFunc(aggFunc: AggFunc): Set[AggFunc] = {
      aggFunc match {
        case Mean => Set(Sum, Count)
        case NonZeroMean => Set(Sum, NonZeroCount)
        case Frequency | TopN => Set(Frequency)
        case _ => Set(aggFunc)
      }
    }

    // Custom decoder for AggFunc
    implicit val aggFuncDecoder: Decoder[AggFunc] = Decoder[String].flatMap { str =>
      Decoder.decodeString.emap { value =>
        AggFunc.fromString(value) match {
          case Some(aggFunc) => Right(aggFunc)
          case None => Left(s"Invalid aggregation function: $value")
        }
      }
    }
  }

  trait AggSpecs {
    def aggField: String

    def aggWindowDay: Int
  }

  case class CategoryFeatAggSpecs(
                                   aggField: String,
                                   aggWindowDay: Int,
                                   topN: Int,
                                   dataType: String,
                                   cardinality: Int,
                                   featureName: String,
                                 ) extends AggSpecs

  object CategoryFeatAggSpecs {
    def apply(aggField: String, aggWindow: Int, topN: Int, dataType: String, cardinality: Int): CategoryFeatAggSpecs = {
      val featureName = s"Top${topN}${aggField}Last${aggWindow}D"
      new CategoryFeatAggSpecs(aggField, aggWindow, topN, dataType, cardinality, featureName)
    }
  }

  case class ContinuousFeatAggSpecs(
                                     aggField: String,
                                     aggWindowDay: Int,
                                     aggFunc: AggFunc,
                                     featureName: String,
                                   ) extends AggSpecs

  object ContinuousFeatAggSpecs {
    def apply(aggField: String, aggWindow: Int, aggFunc: AggFunc): ContinuousFeatAggSpecs = {
      val featureName = s"${aggFunc}${aggField}Last${aggWindow}D"
      new ContinuousFeatAggSpecs(aggField, aggWindow, aggFunc, featureName)
    }
  }

  case class RatioFeatAggSpecs(
                                aggField: String,
                                aggWindowDay: Int,
                                denomField: String,
                                ratioMetrics: String,
                                featureName: String,
                              ) extends AggSpecs

  object RatioFeatAggSpecs {
    def apply(aggField: String, aggWindow: Int, denomField: String, ratioMetrics: String): RatioFeatAggSpecs = {
      val featureName = s"${ratioMetrics}Last${aggWindow}D"
      new RatioFeatAggSpecs(aggField, aggWindow, denomField, ratioMetrics, featureName)
    }
  }

  def getFieldType(df: Dataset[_], fieldName: String): String = {
    df.schema
      .find(_.name == fieldName)
      .map(_.dataType.typeName)
      .getOrElse("None")
  }


  def hashFeature(inputColName: String, dType: String, cardinality: Int): Column = {
    val hashColName = s"${inputColName}Hash"
    // todo: we can refactor this hashing logic by replacing it with `featureSelect` in geronimo package
    dType match {
      case STRING_FEATURE_TYPE => when(col(inputColName).isNotNullOrEmpty, shiftModUdf(xxhash64(col(inputColName)), lit(cardinality))).otherwise(0).alias(hashColName)
      case INT_FEATURE_TYPE | LONG_FEATURE_TYPE => when(col(inputColName).isNotNull, shiftModUdf(col(inputColName), lit(cardinality))).otherwise(0).alias(hashColName)
      case ARRAY_STRING_FEATURE_TYPE => when(col(inputColName).isNotNull, shiftModArrayUdf(transform(col(inputColName), value => xxhash64(value)), lit(cardinality))).otherwise(lit(array())).alias(hashColName)
      case ARRAY_INT_FEATURE_TYPE | ARRAY_LONG_FEATURE_TYPE => transform(col(inputColName), value => when(value.isNotNull, shiftModUdf(value, lit(cardinality))).otherwise(0)).alias(hashColName)
      case FLOAT_FEATURE_TYPE => col(inputColName).alias(hashColName)
      case _ => throw new UnsupportedOperationException(s"Unsupported data type ${dType} with feature ${inputColName}")
    }
  }
}
