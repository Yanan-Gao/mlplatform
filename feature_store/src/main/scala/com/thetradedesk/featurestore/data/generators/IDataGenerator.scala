package com.thetradedesk.featurestore.data.generators

import com.thetradedesk.featurestore.configs.{DataType, FeatureDefinition, UserFeatureMergeDefinition}
import com.thetradedesk.featurestore.constants.FeatureConstants
import com.thetradedesk.featurestore.constants.FeatureConstants.FeatureDataKey
import com.thetradedesk.featurestore.data.loader.FeatureDataLoader
import com.thetradedesk.featurestore.data.metrics.UserFeatureMergeJobTelemetry
import com.thetradedesk.featurestore.entities.Result
import com.thetradedesk.featurestore.shouldConsiderTDID
import com.thetradedesk.featurestore.udfs.BinaryOperations.binaryToLongArrayUdf
import com.thetradedesk.featurestore.utils.FileHelper
import com.thetradedesk.spark.TTDSparkContext.spark
import com.thetradedesk.spark.TTDSparkContext.spark.implicits._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.apache.spark.sql.{Column, DataFrame, SparkSession}
import upickle.default._

import java.time.LocalDateTime
import scala.collection.Seq

abstract class IDataGenerator(implicit sparkSession: SparkSession, telemetry: UserFeatureMergeJobTelemetry) {
  protected val featureDataLoader = new FeatureDataLoader()

  final def generate(dateTime: LocalDateTime, userFeatureMergeDefinition: UserFeatureMergeDefinition): (DataFrame, String) = {
    val preValidationResult = preValidate(dateTime, userFeatureMergeDefinition)
    if (!preValidationResult.success) {
      throw new RuntimeException(s"[Data Generator] data pre validation failed, reason: [${preValidationResult.message}]")
    }

    val dataSource = readFeatureSource(dateTime, userFeatureMergeDefinition).cache()
    val (result, features) = generate(dataSource, userFeatureMergeDefinition)

    //    println(result.explain(true))

    // write schema before we start process data
    val featureSchemaJson = write(features)
    println("feature schema json:")
    println(featureSchemaJson)
    FileHelper.writeStringToFile(userFeatureMergeDefinition.schemaPath(dateTime), featureSchemaJson)(spark)

    val cachedResult = result.cache()

    val metrics = cachedResult
      .where(shouldConsiderTDID(col(userFeatureMergeDefinition.sourceIdKey)))
      .select(length(col(FeatureDataKey)).alias("size"))
      .select('size, when('size <= userFeatureMergeDefinition.config.maxDataSizePerRecord, 1).otherwise(0).alias("label"))
      .agg(sum('label), count('label), avg('size))
      .collect()

    telemetry.recordCount.labels(dateTime.toString, "OutOfBound").set(metrics(0)(0).asInstanceOf[Long])
    telemetry.recordCount.labels(dateTime.toString, "Total").set(metrics(0)(1).asInstanceOf[Long])
    telemetry.averageRecordSize.labels(dateTime.toString).set(metrics(0)(2).asInstanceOf[Double])

    val finalResult = cachedResult.where(length(col(FeatureDataKey)) <= userFeatureMergeDefinition.config.maxDataSizePerRecord)

    val postValidationResult = postValidate(dateTime, dataSource, finalResult, features, userFeatureMergeDefinition)
    if (!postValidationResult.success) {
      throw new RuntimeException(s"[Data Generator] data post validation failed, reason: [${postValidationResult.message}]")
    }

    (finalResult, featureSchemaJson)
  }

  def generate(dataSource: DataFrame, userFeatureMergeDefinition: UserFeatureMergeDefinition): (DataFrame, Seq[Feature])

  private def readFeatureSource(dateTime: LocalDateTime, userFeatureMergeDefinition: UserFeatureMergeDefinition): DataFrame = {
    val featureMap = userFeatureMergeDefinition
      .featureSourceDefinitions
      .flatMap(e => e.features.map(f => (s"${e.name}_${f.name}", f)))
      .toMap

    val df = userFeatureMergeDefinition
      .featureSourceDefinitions
      .map(e => (e, featureDataLoader.readFeatureSourceData(dateTime, e)(sparkSession)))
      .map(p =>
        p._2.select(
          p._1.features.map(
            e => convertFeatureType(col(e.name), p._2.schema(e.name), e).alias(s"${p._1.name}_${e.name}"))
            :+ col(p._1.idKey).alias(userFeatureMergeDefinition.sourceIdKey): _*)) // rename key to TDID
      .reduce(_.join(_, Seq(userFeatureMergeDefinition.sourceIdKey), "outer"))

    df.select(df.columns.filter(_ != userFeatureMergeDefinition.sourceIdKey).map(e => coalesce(col(e), lit(featureMap(e).typedDefaultValue)).alias(e)) :+ col(userFeatureMergeDefinition.sourceIdKey): _*)
  }

  // TODO check data size, array size...
  // TODO support default values
  private def convertFeatureType(column: Column, structField: StructField, featureDefinition: FeatureDefinition): Column = {
    featureDefinition match {
      case FeatureDefinition(_, DataType.Byte, 0, _, _, _)
           | FeatureDefinition(_, DataType.Short, 0, _, _, _)
           | FeatureDefinition(_, DataType.Int, 0, _, _, _)
           | FeatureDefinition(_, DataType.Long, 0, _, _, _) =>
        if (structField.dataType.isInstanceOf[ByteType]
          || structField.dataType.isInstanceOf[ShortType]
          || structField.dataType.isInstanceOf[IntegerType]
          || structField.dataType.isInstanceOf[LongType]) {
          column.cast(LongType)
        } else {
          throw new RuntimeException(s"[Data Generator] feature type must be an instance of IntegralType")
        }
      case FeatureDefinition(_, DataType.Float, 0, _, _, _)
           | FeatureDefinition(_, DataType.Double, 0, _, _, _) =>
        if (structField.dataType.isInstanceOf[FloatType] || structField.dataType.isInstanceOf[DoubleType]) {
          column.cast(DoubleType)
        } else {
          throw new RuntimeException(s"[Data Generator] feature type must be an instance of FractionalType")
        }
      case FeatureDefinition(_, DataType.Bool, 0, _, _, _) =>
        if (!structField.dataType.isInstanceOf[BooleanType]) {
          throw new RuntimeException(s"[Data Generator] feature type must be an instance of BooleanType")
        }
        column
      case FeatureDefinition(_, DataType.Map, 0, _, _, _)
           | FeatureDefinition(_, DataType.String, 0, _, _, _) =>
        if (!structField.dataType.isInstanceOf[StringType]) {
          throw new RuntimeException(s"[Data Generator] feature type must be an instance of StringType")
        }
        column
      case FeatureDefinition(_, DataType.Byte, _, _, _, _)
           | FeatureDefinition(_, DataType.Short, _, _, _, _)
           | FeatureDefinition(_, DataType.Int, _, _, _, _)
           | FeatureDefinition(_, DataType.Long, _, _, _, _) =>
        structField.dataType match {
          case BinaryType => binaryToLongArrayUdf(column)
          case ArrayType(ByteType, _)
               | ArrayType(ShortType, _)
               | ArrayType(IntegerType, _)
               | ArrayType(LongType, _) => column.cast(ArrayType(LongType))
          case _ => throw new RuntimeException(s"[Data Generator] feature type ${structField.dataType} is not supported for integral arrays")
        }
      case FeatureDefinition(_, DataType.Float, _, _, _, _)
           | FeatureDefinition(_, DataType.Double, _, _, _, _) =>
        structField.dataType match {
          case ArrayType(ByteType, _)
               | ArrayType(ShortType, _)
               | ArrayType(IntegerType, _)
               | ArrayType(LongType, _)
               | ArrayType(FloatType, _)
               | ArrayType(DoubleType, _) => column.cast(ArrayType(DoubleType))
          case _ => throw new RuntimeException(s"[Data Generator] feature type ${structField.dataType} is not supported for integral arrays")
        }
      case _ =>
        throw new RuntimeException(s"[Data Generator] unsupported feature definition $featureDefinition")
    }
  }

  /**
   * validate user feature merge definition correct
   * validate all features in production are configured
   *
   * @param dataFrame
   * @param schema
   * @return
   */
  protected def preValidate(dateTime: LocalDateTime, userFeatureMergeDefinition: UserFeatureMergeDefinition): Result = {
    if (!userFeatureMergeDefinition.validate.success) {
      userFeatureMergeDefinition.validate
    } else {
      Result.succeed()
    }
  }

  /**
   * validate data and schema generated
   *
   * @param dataFrame
   * @param schema
   * @return
   */
  protected def postValidate(dateTime: LocalDateTime, dataSource: DataFrame, result: DataFrame, features: Seq[Feature], userFeatureMergeDefinition: UserFeatureMergeDefinition): Result = {
    Result.succeed()
  }
}
