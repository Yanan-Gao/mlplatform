package com.thetradedesk.featurestore.data.generators

import com.thetradedesk.featurestore.configs.{DataType, UserFeatureMergeConfiguration, UserFeatureMergeDefinition}
import com.thetradedesk.featurestore.constants.FeatureConstants
import com.thetradedesk.featurestore.constants.FeatureConstants.{BitsOfByte, BytesToKeepAddress, FeatureDataKey, UserIDKey}
import com.thetradedesk.featurestore.data.generators.CustomBufferDataGenerator.refineDataFrame
import com.thetradedesk.featurestore.data.metrics.UserFeatureMergeJobTelemetry
import com.thetradedesk.featurestore.{dateTime, shouldConsiderTDID3}
import com.thetradedesk.featurestore.entities.Result
import com.thetradedesk.featurestore.udfs.BinaryOperations.binaryToLongArrayUdf
import com.thetradedesk.featurestore.utils.SchemaComparer
import com.thetradedesk.spark.util.TTDConfig.config
import com.thetradedesk.spark.util.prometheus.PrometheusClient
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.apache.spark.sql.{Column, DataFrame, SparkSession}

import java.nio.{ByteBuffer, ByteOrder}
import java.nio.charset.StandardCharsets
import java.time.LocalDateTime
import scala.reflect.ClassTag
import scala.reflect.runtime.universe._

class CustomBufferDataGenerator(implicit sparkSession: SparkSession, telemetry: UserFeatureMergeJobTelemetry) extends IDataGenerator {
  /**
   * validate user feature merge definition correct
   * validate all features in production are configured
   *
   * @param dataFrame
   * @param schema
   * @return
   */
  override protected def preValidate(dateTime: LocalDateTime, userFeatureMergeDefinition: UserFeatureMergeDefinition): Result = {
    val superResult = super.preValidate(dateTime, userFeatureMergeDefinition)
    if (!superResult.success) {
      superResult
    } else {
      if (userFeatureMergeDefinition.featureSourceDefinitions.exists(e => e.features.exists(_.dtype == DataType.Map))) {
        Result.failed("Map is not supported, please use two arrays to mimic map")
      } else {
        Result.succeed()
      }
    }
  }

  /**
   * generate data, convert original features into byte array
   * TODO optimize and extract the data encode/decode logic into a separate class
   * TDOD use stack struct to push items and construct results to reduce duplicated byte array creation anc copy
   * @param dataSource
   * @param userFeatureMergeDefinition
   * @return
   */
  override def generate(dataSource: DataFrame, userFeatureMergeDefinition: UserFeatureMergeDefinition): (DataFrame, Seq[Feature]) = {
    var index = -1
    var offset = 7
    var prevSize = 0

    val nextStep = (dataType: DataType, arrayLength: Int) => {
      if (dataType == DataType.Bool) {
        if (offset == 7) {
          offset = 0
          index += 1
        } else {
          offset += 1
        }
      } else {
        if (offset != 0) {
          index += 1
          offset = 0
        }
        index += prevSize

        prevSize = CustomBufferDataGenerator.byteWidthOfArray(dataType, arrayLength)
      }
      checkBounds(index, userFeatureMergeDefinition.config)
    }

    val featureDefinitions = userFeatureMergeDefinition
      .featureSourceDefinitions
      .flatMap(e => e.features.map((e.name, _)))
      .sortWith((e1, e2) => {
        // string should be processed at the last
        // then variable-length array should be processed
        // then fixed array
        // then different types
        // then follow name rules
        if (e1._2.dtype == DataType.String && e2._2.dtype != DataType.String) {
          false
        } else if (e1._2.dtype != DataType.String && e2._2.dtype == DataType.String) {
          true
        } else if (e1._2.arrayLength != e2._2.arrayLength) {
          if (e1._2.arrayLength == 1) {
            false
          } else if (e2._2.arrayLength == 1) {
            true
          } else {
            e1._2.arrayLength - e2._2.arrayLength < 0
          }
        } else if (e1._2.dtype != e2._2.dtype) {
          e1._2.dtype < e2._2.dtype
        } else if (e1._1 != e2._1) {
          e1._1.compareTo(e2._1) < 0
        } else {
          e1._2.name.compareTo(e2._2.name) < 0
        }
      })

    val lastFeature = (featureDefinitions.last._1, featureDefinitions.last._2.name)

      val features = featureDefinitions.map(
        e => {
          nextStep(e._2.dtype, e._2.arrayLength)
          val isLastFeature = e._2.name == lastFeature._2 && e._1 == lastFeature._1
          e._2.dtype match {
            case DataType.Bool => Feature(e._2.name, e._1, index, offset, DataType.Bool, isLastFeature = isLastFeature)
            case _ => Feature(e._2.name, e._1, index, if (e._2.arrayLength > 1) e._2.arrayLength else 0, e._2.dtype, isArray = e._2.arrayLength != 0, isLastFeature = isLastFeature)
          }
        }
      )
      .toSeq

    val df = dataSource
      .select(col(FeatureConstants.UserIDKey), dataToByteArray(features, userFeatureMergeDefinition.config.maxDataSizePerRecord).alias(FeatureDataKey))

    (df, features)
  }

  // TODO block when size is larger than maxDataSizePerRecord in advance
  private def dataToByteArray(features: Seq[Feature], maxDataSizePerRecord: Int) = {
    var columns = Seq.empty[Column]
    // boolean features
    val boolFeatures = features.filter(_.dataType == DataType.Bool)
    if (boolFeatures.nonEmpty) {
      columns = columns :+ boolDataToByteArrayUdf(boolFeatures.length)(array(boolFeatures.map(e => col(e.fullName)): _*))
    }

    // integral features
    val integralFeatures = features
      .filter(e => !e.isArray && (e.dataType >= DataType.Byte && e.dataType <= DataType.Long))

    if (integralFeatures.nonEmpty) {
      columns = columns :+ numericDataToByteArrayUdf[Long](integralFeatures: _*)(TypeTag.Long)(array(integralFeatures.map(e => col(e.fullName)): _*))
    }

    // fractional features
    val fractionalFeatures = features
      .filter(e => !e.isArray && (e.dataType >= DataType.Float && e.dataType <= DataType.Double))

    if (fractionalFeatures.nonEmpty) {
      columns = columns :+ numericDataToByteArrayUdf[Double](fractionalFeatures: _*)(TypeTag.Double)(array(fractionalFeatures.map(e => col(e.fullName)): _*))
    }

    // fixed-length array features
    features
      .filter(e => e.isArray && e.offset >= 1 && (e.dataType >= DataType.Byte && e.dataType <= DataType.Double))
      .foreach(f => {
        if (f.dataType >= DataType.Byte && f.dataType <= DataType.Long) {
          // fixed-length integral array features
          columns = columns :+ numericArrayDataToByteArrayUdf[Long](f)(TypeTag.Long)(col(f.fullName))
        } else {
          // fixed-length fractional array features
          columns = columns :+ numericArrayDataToByteArrayUdf[Double](f)(TypeTag.Double)(col(f.fullName))
        }
      })

    var variableLengthColumns = Seq.empty[Column]

    // variable-length integral array features
    features
      .filter(e => e.isArray && e.offset == 0 && (e.dataType >= DataType.Byte && e.dataType <= DataType.Long))
      .foreach(f => variableLengthColumns = variableLengthColumns :+ numericArrayDataToByteArrayUdf[Long](f)(TypeTag.Long)(col(f.fullName)))

    // variable-length fractional array features
    features
      .filter(e => e.isArray && e.offset == 0 && (e.dataType >= DataType.Float && e.dataType <= DataType.Double))
      .foreach(f => variableLengthColumns = variableLengthColumns :+ numericArrayDataToByteArrayUdf[Double](f)(TypeTag.Double)(col(f.fullName)))

    // string features
    features
      .filter(e => !e.isArray && e.dataType == DataType.String)
      .foreach(f => variableLengthColumns = variableLengthColumns :+ stringDataToByteArrayUdf(col(f.fullName)))

    if (variableLengthColumns.nonEmpty) {
      val variableDataAddressColumn = buildVariableDataAddress(features.last.index + BytesToKeepAddress, variableLengthColumns.length, maxDataSizePerRecord)(array(variableLengthColumns.map(length): _*))
      columns = (columns :+ variableDataAddressColumn) ++ variableLengthColumns
    }

    concat(columns: _*)
  }

  private def buildVariableDataAddress(initialOffset: Int, featureLength: Int, maxDataSizePerRecord:Int) = {
    val length = featureLength * BytesToKeepAddress
    udf({ (values: Seq[Int]) => {
      val byteBuffer = ByteBuffer.allocate(length).order(ByteOrder.LITTLE_ENDIAN)
      var index = 0
      var offset = initialOffset
      values.foreach(
        e => {
          byteBuffer.putShort(index, offset.asInstanceOf[Short])
          index += BytesToKeepAddress
          offset += e
        }
      )
      byteBuffer.array()
    }
    })
  }

  private def numericDataToByteArrayUdf[Type](features: Feature*)(implicit ttag: TypeTag[Type]) = {
    val length = features.map(e => CustomBufferDataGenerator.byteWidthOfArray(e.dataType, e.offset)).sum
    val dataTypes = features.map(_.dataType)
    udf({ (values: Seq[Type]) => {
      assert(dataTypes.length == values.length, "features length is not equal to values length")

      var index = 0
      val byteBuffer = ByteBuffer.allocate(length).order(ByteOrder.LITTLE_ENDIAN)
      values.zip(dataTypes).foreach {
        case (value: java.lang.Long, DataType.Byte) =>
          byteBuffer.put(index, value.byteValue())
          index += 1
        case (value: java.lang.Long, DataType.Short) =>
          byteBuffer.putShort(index, value.shortValue())
          index += 2
        case (value: java.lang.Long, DataType.Int) =>
          byteBuffer.putInt(index, value.intValue())
          index += 4
        case (value: java.lang.Long, DataType.Long) =>
          byteBuffer.putLong(index, value)
          index += 8
        case (value: java.lang.Double, DataType.Float) =>
          byteBuffer.putFloat(index, value.floatValue())
          index += 4
        case (value: java.lang.Double, DataType.Double) =>
          byteBuffer.putDouble(index, value)
          index += 8
        case value => throw new RuntimeException(s"it should not happen with ${value}")
      }

      byteBuffer.array()
    }
    })
  }

  private def numericArrayDataToByteArrayUdf[Type](feature: Feature)(implicit ttag: TypeTag[Type]) = {
    val offset = feature.offset
    val size = CustomBufferDataGenerator.byteWidthOfType(feature.dataType)
    udf({ (values: Seq[Type]) => {
      assert(offset == 0 || offset == values.length, "features length is not equal to values length")

      var index = 0
      val byteBuffer = ByteBuffer.allocate(size * values.length).order(ByteOrder.LITTLE_ENDIAN)
      feature.dataType match {
        case DataType.Byte =>
          values.foreach(e => {
            byteBuffer.put(index, e.asInstanceOf[java.lang.Long].byteValue())
            index += 1
          })
        case DataType.Short =>
          values.foreach(f => {
            byteBuffer.putShort(index, f.asInstanceOf[java.lang.Long].shortValue())
            index += 2
          })
        case DataType.Int =>
          values.foreach(g => {
            byteBuffer.putInt(index, g.asInstanceOf[java.lang.Long].intValue())
            index += 4
          })
        case DataType.Long =>
          values.foreach(h => {
            byteBuffer.putLong(index, h.asInstanceOf[java.lang.Long])
            index += 8
          })
        case DataType.Float =>
          values.foreach(i => {
            byteBuffer.putFloat(index, i.asInstanceOf[java.lang.Double].floatValue())
            index += 4
          })
        case DataType.Double =>
          values.foreach(j => {
            byteBuffer.putDouble(index, j.asInstanceOf[java.lang.Double])
            index += 8
          })
        case value => throw new RuntimeException(s"it should not happen with ${value}")
      }

      byteBuffer.array()
    }
    })
  }

  private def boolDataToByteArrayUdf(featureLen: Int) = {
    val length = (featureLen + BitsOfByte - 1) / BitsOfByte
    udf({ (values: Seq[Boolean]) => {
      val buffer = Array.fill[Byte](length)(0)
      values
        .zipWithIndex
        .foreach(
          e => if (e._1) {
            val idx = e._2 >> 3
            val off = e._2 & 0x7
            buffer.update(idx, (buffer(idx) | (1 << off)).toByte)
          }
        )
      buffer
    }
    })
  }

  private val stringDataToByteArrayUdf = udf({ (value: String) => {
    value.getBytes(StandardCharsets.UTF_8)
  }
  })

  private def checkBounds(index: Int, config: UserFeatureMergeConfiguration) = {
    if (index >= config.maxDataSizePerRecord) {
      throw new IndexOutOfBoundsException(s"data size ${index} is out of bounds, maximal ${config.maxDataSizePerRecord} bytes")
    }
  }

  /**
   *
   * @param df
   * @param features
   * @param castType cast byte/short/int to long, float to double for better comparison
   * @return
   */
  def readFromDataFrame(df: DataFrame, features: Seq[Feature], castType: Boolean = false): DataFrame = {
    val featureColumns = features.zipWithIndex.map(
      e => {
        val feature = e._1
        (feature.dataType, feature.isArray) match {
          case (DataType.Bool, false) => readFromDataFrameUdf[Boolean](feature)(TypeTag.Boolean)(col(FeatureDataKey)).alias(feature.fullName)
          case (DataType.String, false) => readFromDataFrameUdf[String](feature)(typeTag[String])(col(FeatureDataKey)).alias(feature.fullName)
          case (DataType.Byte, false) =>
            if (castType) readFromDataFrameUdf[Byte](feature)(TypeTag.Byte)(col(FeatureDataKey)).cast(LongType).alias(feature.fullName)
            else readFromDataFrameUdf[Byte](feature)(TypeTag.Byte)(col(FeatureDataKey)).alias(feature.fullName)
          case (DataType.Short, false) =>
            if (castType) readFromDataFrameUdf[Short](feature)(TypeTag.Short)(col(FeatureDataKey)).cast(LongType).alias(feature.fullName)
            else readFromDataFrameUdf[Short](feature)(TypeTag.Short)(col(FeatureDataKey)).alias(feature.fullName)
          case (DataType.Int, false) =>
            if (castType) readFromDataFrameUdf[Int](feature)(TypeTag.Int)(col(FeatureDataKey)).cast(LongType).alias(feature.fullName)
            else readFromDataFrameUdf[Int](feature)(TypeTag.Int)(col(FeatureDataKey)).alias(feature.fullName)
          case (DataType.Long, false) => readFromDataFrameUdf[Long](feature)(TypeTag.Long)(col(FeatureDataKey)).alias(feature.fullName)
          case (DataType.Float, false) =>
            if (castType) readFromDataFrameUdf[Float](feature)(TypeTag.Float)(col(FeatureDataKey)).cast(DoubleType).alias(feature.fullName)
            else readFromDataFrameUdf[Float](feature)(TypeTag.Float)(col(FeatureDataKey)).alias(feature.fullName)
          case (DataType.Double, false) => readFromDataFrameUdf[Double](feature)(TypeTag.Double)(col(FeatureDataKey)).alias(feature.fullName)
          case (DataType.Byte, true) =>
            if (castType) binaryToLongArrayUdf(readFromDataFrameUdf[Array[Byte]](feature)(typeTag[Array[Byte]])(col(FeatureDataKey))).alias(feature.fullName)
            else readFromDataFrameUdf[Array[Byte]](feature)(typeTag[Array[Byte]])(col(FeatureDataKey)).alias(feature.fullName)
          case (DataType.Short, true) =>
            if (castType)readFromDataFrameUdf[Array[Short]](feature)(typeTag[Array[Short]])(col(FeatureDataKey)).cast(ArrayType(LongType)).alias(feature.fullName)
            else readFromDataFrameUdf[Array[Short]](feature)(typeTag[Array[Short]])(col(FeatureDataKey)).alias(feature.fullName)
          case (DataType.Int, true) =>
            if (castType) readFromDataFrameUdf[Array[Int]](feature)(typeTag[Array[Int]])(col(FeatureDataKey)).cast(ArrayType(LongType)).alias(feature.fullName)
            else readFromDataFrameUdf[Array[Int]](feature)(typeTag[Array[Int]])(col(FeatureDataKey)).alias(feature.fullName)
          case (DataType.Long, true) => readFromDataFrameUdf[Array[Long]](feature)(typeTag[Array[Long]])(col(FeatureDataKey)).alias(feature.fullName)
          case (DataType.Float, true) =>
            if (castType) readFromDataFrameUdf[Array[Float]](feature)(typeTag[Array[Float]])(col(FeatureDataKey)).cast(ArrayType(DoubleType)).alias(feature.fullName)
            else readFromDataFrameUdf[Array[Float]](feature)(typeTag[Array[Float]])(col(FeatureDataKey)).alias(feature.fullName)
          case (DataType.Double, true) => readFromDataFrameUdf[Array[Double]](feature)(typeTag[Array[Double]])(col(FeatureDataKey)).alias(feature.fullName)
          case _ => throw new UnsupportedOperationException(s"type ${feature.dataType} isArray ${feature.isArray}  is not supported")
        }
      }
    )
    df.select(featureColumns :+ col(FeatureConstants.UserIDKey): _*)
  }

  private def readFromDataFrameUdf[T](feature: Feature)(implicit ttag: TypeTag[T]) = {
    udf({
      data: Array[Byte] => CustomBufferDataGenerator.read(data, feature).asInstanceOf[T]
    })
  }

  /**
   * validate data and schema generated
   *
   * @param dataFrame
   * @param schema
   * @return
   */
  override protected def postValidate(dateTime: LocalDateTime, dataSource: DataFrame, result: DataFrame, features: Seq[Feature], userFeatureMergeDefinition: UserFeatureMergeDefinition): Result = {
    val postValidationResult = super.postValidate(dateTime, dataSource, result, features, userFeatureMergeDefinition)
    if (!postValidationResult.success) {
      return postValidationResult
    }

    val shouldConsiderTDIDUDF = shouldConsiderTDID3(config.getInt("postValidationSampleHit", default = 10000), "fpVq")(_)

    val origin = dataSource.where(shouldConsiderTDIDUDF(col(FeatureConstants.UserIDKey)))

    val other = this.readFromDataFrame(result.where(shouldConsiderTDIDUDF(col(FeatureConstants.UserIDKey))), features, true)

    compareSmallDatasets(refineDataFrame(other), refineDataFrame(origin))
  }

  private def compareSmallDatasets(actualDS: DataFrame, expectedDS: DataFrame):Result = {
    if (!SchemaComparer.equals(actualDS.schema, expectedDS.schema, false, false)) {
      Result.failed("schema mismatch")
    } else {
      val a = actualDS.collect()
      val e = expectedDS.collect()
      if (!a.sameElements(e)) {
        Result.failed("data mismatch")
      } else {
        Result.succeed()
      }
    }
  }
}

object CustomBufferDataGenerator {
  def read(data: Array[Byte], feature: Feature): Any = {
    val byteBuffer = ByteBuffer.wrap(data).order(ByteOrder.LITTLE_ENDIAN)
    if (!feature.isArray && feature.dataType == DataType.Bool) {
      return (byteBuffer.get(feature.index) & (1 << feature.offset)) != 0
    }
    if (!feature.isArray && feature.dataType >= DataType.Byte && feature.dataType <= DataType.Double) {
      return readValue(byteBuffer, feature.dataType, feature.index)
    }

    val start = if (feature.offset > 1) feature.index // fixed length array
    else byteBuffer.getShort(feature.index).intValue()  // var length feature

    val length = if (feature.offset > 1) byteWidthOfArray(feature.dataType, feature.offset) // fixed length array
    else if (feature.isLastFeature) data.length - start  // last var length feature
    else byteBuffer.getShort(feature.index + BytesToKeepAddress) - start // non-last var length feature

    if (!feature.isArray && feature.dataType == DataType.String) {
      return new String(data, start, length, StandardCharsets.UTF_8)
    }

    if (feature.isArray && feature.dataType >= DataType.Byte && feature.dataType <= DataType.Double) {
      return readArrayValue(byteBuffer, feature.dataType, start, length)
    }

    throw new UnsupportedOperationException(s"type ${feature.dataType} isArray ${feature.isArray}  is not supported")
  }

  private def readArrayValue(byteBuffer: ByteBuffer, dataType: DataType, index: Int, byteLength: Int) = {
    dataType match {
      case DataType.Byte =>
        fillArray[Byte](byteBuffer, dataType, index, byteLength)
      case DataType.Short =>
        fillArray[Short](byteBuffer, dataType, index, byteLength)
      case DataType.Int =>
        fillArray[Int](byteBuffer, dataType, index, byteLength)
      case DataType.Long =>
        fillArray[Long](byteBuffer, dataType, index, byteLength)
      case DataType.Float =>
        fillArray[Float](byteBuffer, dataType, index, byteLength)
      case DataType.Double =>
        fillArray[Double](byteBuffer, dataType, index, byteLength)
      case _ => throw new UnsupportedOperationException(s"type ${dataType}  is not supported")
    }
  }

  def fillArray[T](byteBuffer: ByteBuffer, dataType: DataType, index: Int, byteLength: Int)(implicit m: ClassTag[T]): Array[T] = {
    val resultLength = lengthOfType(dataType, byteLength)
    val results = new Array[T](resultLength)
    val width = byteWidthOfType(dataType)
    var idx = index
    for (i <- 0 until resultLength) {
      results.update(i, readValue(byteBuffer, dataType, idx).asInstanceOf[T])
      idx += width
    }
    results
  }

  def byteWidthOfArray(dataType: DataType, arrayLength: Int) = {
    if (arrayLength == 1) {
      // var length data
      BytesToKeepAddress
    } else {
      val length = math.max(arrayLength, 1)
      dataType match {
        case DataType.Bool => length
        case DataType.Byte => length
        case DataType.Short => length << 1
        case DataType.Int => length << 2
        case DataType.Long => length << 3
        case DataType.Float => length << 2
        case DataType.Double => length << 3
        case _ => BytesToKeepAddress // extra list to hold address
      }
    }
  }

  def byteWidthOfType(dataType: DataType) = {
    dataType match {
      case DataType.Bool => 1
      case DataType.Byte => 1
      case DataType.Short => 2
      case DataType.Int => 4
      case DataType.Long => 8
      case DataType.Float => 4
      case DataType.Double => 8
      case _ => 0 // unknown
    }
  }

  def lengthOfType(dataType: DataType, byteLength: Int) = {
    dataType match {
      case DataType.Byte => byteLength
      case DataType.Short => byteLength >> 1
      case DataType.Int => byteLength >> 2
      case DataType.Long => byteLength >> 3
      case DataType.Float => byteLength >> 2
      case DataType.Double => byteLength >> 3
      case _ => 0 // unknown
    }
  }

  def readValue(byteBuffer: ByteBuffer, dataType: DataType, index: Int): Any = {
    dataType match {
      case DataType.Byte => byteBuffer.get(index)
      case DataType.Short => byteBuffer.getShort(index)
      case DataType.Int => byteBuffer.getInt(index)
      case DataType.Long => byteBuffer.getLong(index)
      case DataType.Float => byteBuffer.getFloat(index)
      case DataType.Double => byteBuffer.getDouble(index)
      case _ => throw new UnsupportedOperationException(s"type ${dataType}  is not supported")
    }
  }

  def refineDataFrame(df: DataFrame) : DataFrame = {
    setNullableState(
      df
        .select(df.columns.sorted.map(col): _*)
        .orderBy(col(UserIDKey).asc)
    )
  }

  def setNullableState(df: DataFrame, nullable: Boolean = false, containsNull: Boolean = false): DataFrame = {
    val schema = df.schema
    val newSchema = StructType(schema.map {
      case StructField(c, t: ArrayType, _, m) => StructField(c, ArrayType(t.elementType, containsNull = containsNull), nullable = nullable, m)
      case StructField(c, t, _, m) => StructField(c, t, nullable = nullable, m)
    })

    df.sqlContext.createDataFrame(df.rdd, newSchema)
  }
}