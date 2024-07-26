package com.thetradedesk.featurestore.data.cbuffer

import com.thetradedesk.featurestore.configs.DataType
import com.thetradedesk.featurestore.data.cbuffer.CBufferConstants.{DefaultMaxRecordSize, ArrayLengthKey}
import com.thetradedesk.featurestore.data.generators.CustomBufferDataGenerator
import org.apache.spark.sql.Column
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.types.{ArrayType, BinaryType, BooleanType, ByteType, DoubleType, FloatType, IntegerType, LongType, MetadataBuilder, ShortType, StringType, StructField, StructType, DataType => SparkDataType}

object SchemaHelper {
  def inferFeature(structType: StructType): Array[CBufferFeature] = {
    organizeFeatures(
      structType.fields
        .map(e => {
          val (dataType, isArray) = toInternalDataType(e.dataType)
          (e.name, dataType, isArray, if (isArray && e.metadata.contains(ArrayLengthKey)) e.metadata.getLong(ArrayLengthKey).toInt else 0)
        }))
  }

  def inferSchema(features: Array[CBufferFeature]): StructType = {
    StructType(
      features.map {
        case CBufferFeature(name, _, _, dataType: DataType, false) =>
          StructField(name, dataType = toSparkDataType(dataType))
        case CBufferFeature(name, _, arrayLength, dataType: DataType, true) =>
          StructField(name, dataType = ArrayType(toSparkDataType(dataType)), metadata = new MetadataBuilder().putLong(ArrayLengthKey, arrayLength).build())
      })
  }

  def indicateArrayLength(columnName: String, arrayLength: Int) : Column = {
    val metadata = new MetadataBuilder().putLong(ArrayLengthKey, arrayLength).build()
    col(columnName).as(columnName, metadata=metadata)
  }

  private def toSparkDataType(dataType: DataType) = dataType match {
    case DataType.Bool => BooleanType
    case DataType.Byte => ByteType
    case DataType.Short => ShortType
    case DataType.Int => IntegerType
    case DataType.Long => LongType
    case DataType.Float => FloatType
    case DataType.Double => DoubleType
    case DataType.String => StringType
    case _ => throw new UnsupportedOperationException(s"type ${dataType} is not supported")
  }

  /**
   * @return (spark data type, isArray)
   */
  private def toInternalDataType(dataType: SparkDataType, topTile: Boolean = true): (DataType, Boolean) = dataType match {
    case BooleanType => (DataType.Bool, false)
    case ByteType => (DataType.Byte, false)
    case ShortType => (DataType.Short, false)
    case IntegerType => (DataType.Int, false)
    case LongType => (DataType.Long, false)
    case FloatType => (DataType.Float, false)
    case DoubleType => (DataType.Double, false)
    case StringType => (DataType.String, false)
    case BinaryType => (DataType.Byte, true)
    case ArrayType(elementType, _) =>
      if (topTile)
        (toInternalDataType(elementType, topTile = false)._1, true)
      else throw new UnsupportedOperationException(s"nested array is not supported")
    case _ => throw new UnsupportedOperationException(s"type ${dataType} is not supported")
  }

  // name, dataType, isArray, arrayLength
  def organizeFeatures(featureDefs: Array[(String, DataType, Boolean, Int)]): Array[CBufferFeature] = {
    var index = -1
    var offset = 7
    var prevSize = 0

    val nextStep = (dataType: DataType, isArray: Boolean, arrayLength: Int) => {
      if (dataType == DataType.Bool) {
        if (offset == 7) {
          offset = 0
          index += 1
        } else {
          offset += 1
        }
      } else {
        if (offset != 7 || index == -1) {
          index += 1
          offset = 7
        }
        index += prevSize

        // todo optimize var array length == 1 definition
        prevSize = CustomBufferDataGenerator.byteWidthOfArray(dataType, if (isArray && arrayLength == 0) 1 else arrayLength)
      }
      checkBounds(index, DefaultMaxRecordSize)
    }

    featureDefs
      .map(e => (e, assignOrder(e)))
      .sortWith((e1, e2) => {
        if (e1._2 != e2._2) {
          e1._2 < e2._2
        } else {
          e1._1._1.compareTo(e2._1._1) < 0
        }
      }).map(_._1)
      .map(
        e => {
          nextStep(e._2, e._3, e._4)
          e._2 match {
            case DataType.Bool => CBufferFeature(e._1, index, offset, DataType.Bool, e._3)
            case _ => CBufferFeature(e._1, index, if (e._4 > 1) e._4 else 0, e._2, e._3)
          }
        }
      )
      .toArray
  }

  private def checkBounds(index: Int, maxDataSizePerRecord: Int) = {
    if (index >= maxDataSizePerRecord) {
      throw new IndexOutOfBoundsException(s"data size ${index} is out of bounds, maximal ${maxDataSizePerRecord} bytes")
    }
  }

  private def assignOrder(e: (String, DataType, Boolean, Int)): Int = e match {
    case (_, DataType.String, false, _) => Int.MaxValue
    case (_, dataType: DataType, false, _) => dataType.value
    case (_, dataType: DataType, true, 0) // var-length array
      if dataType >= DataType.Byte && dataType <= DataType.Double => dataType.value << 24
    case (_, dataType: DataType, true, arrayLength: Int) // fixed length array
      if dataType >= DataType.Byte && dataType <= DataType.Double && arrayLength < (1 << 16) => (dataType.value << 16) + arrayLength
    case (name: String, dataType: DataType, isArray: Boolean, arrayLength: Int)
    => throw new UnsupportedOperationException(s"name ${name}, dataType ${dataType}, isArray ${isArray}, arrayLength ${arrayLength} is not supported")
  }
}
