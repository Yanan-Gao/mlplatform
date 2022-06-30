package com.thetradedesk.audience.transform

import com.thetradedesk.geronimo.shared.{FLOAT_FEATURE_TYPE, INT_FEATURE_TYPE, STRING_FEATURE_TYPE, shiftModUdf}
import com.thetradedesk.spark.sql.SQLFunctions.ColumnExtensions
import org.apache.spark.sql.{Column, Dataset}
import org.apache.spark.sql.functions.{col, concat, lit, when, xxhash64}
import com.thetradedesk.spark.TTDSparkContext.spark.implicits._

import scala.reflect.runtime.universe._
import scala.annotation.meta.field

object ModelFeatureTransform {
  val LONG_FEATURE_TYPE = "long";
  def modelFeatureTransform[T <: Product : Manifest](origin: Dataset[_]): Dataset[T] = {
    origin.select(featureSelect[T]: _*).as[T]
  }

  def featureSelect[T <: Product : Manifest]: Array[Column] = {
    typeOf[T].members.collect {
      case t: TermSymbol if t.isVal =>
        val convertedColumns: List[Column] = t.annotations.collect {
          case annotation if
            tryGetAnnotationName(annotation) == classOf[FeatureDesc].getName =>
            annotation.tree.children.tail match {
              case List(Literal(Constant(name: String)), Literal(Constant(dtype: String)), Literal(Constant(cardinality: Int))) =>
                dtype match {
                  case STRING_FEATURE_TYPE => when(col(name).isNotNullOrEmpty, shiftModUdf(xxhash64(col(name)), lit(cardinality))).otherwise(0).alias(t.name.toString.trim)
                  case INT_FEATURE_TYPE => when(col(name).isNotNull, shiftModUdf(col(name), lit(cardinality))).otherwise(0).alias(t.name.toString.trim)
                  case LONG_FEATURE_TYPE => when(col(name).isNotNull, shiftModUdf(col(name), lit(cardinality))).otherwise(0).alias(t.name.toString.trim)
                  case FLOAT_FEATURE_TYPE => col(name).alias(t.name.toString.trim)
                  case _ => throw new Exception(s"Unsupported data type ${dtype} with feature ${name}")
                }
            }
        }
        if (convertedColumns.isEmpty) {
          List(col(t.name.toString.trim))
        } else {
          convertedColumns
        }
    }.flatten.toArray
  }

  def tryGetAnnotationName(annotation: Annotation): String = {
    try {
      annotation.tree.children.head.children.head.children.head.toString()
    } catch {
      case _: Throwable => ""
    }
  }
}

/**
 * define how to transform filed from given inputs
 * @param name must be constant value
 * @param dtype must be constant value, currently support "string", "int", "float"
 * @param cardinality default can set to be 0
 */
@field case class FeatureDesc(name: String,
                              dtype: String,
                              cardinality: Int
                             ) extends scala.annotation.StaticAnnotation {}