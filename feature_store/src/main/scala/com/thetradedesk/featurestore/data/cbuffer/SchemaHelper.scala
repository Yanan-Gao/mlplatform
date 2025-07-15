package com.thetradedesk.featurestore.data.cbuffer

import com.thetradedesk.featurestore.configs.DataType
import com.thetradedesk.featurestore.data.cbuffer.CBufferConstants.{ArrayLengthKey, DefaultMaxRecordSize}
import com.thetradedesk.featurestore.data.generators.CustomBufferDataGenerator
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.util.CaseInsensitiveMap
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.types.{ArrayType, BinaryType, BooleanType, ByteType, DateType, DoubleType, FloatType, IntegerType, LongType, MetadataBuilder, ShortType, StringType, StructField, StructType, TimestampType, DataType => SparkDataType}
import org.apache.spark.sql.{Column, DataFrame, DataFrameReader, DataFrameWriter}

object SchemaHelper {
  def inferFeature(structType: StructType, columnBased: Boolean): Array[CBufferFeature] = {
    organizeFeatures(
      structType.fields
        .map(e => {
          val (dataType, isArray) = toInternalDataType(e.dataType)
          (e.name, dataType, isArray, if (isArray && e.metadata.contains(ArrayLengthKey)) e.metadata.getLong(ArrayLengthKey).toInt else 0)
        }), columnBased)
  }

  def inferSchema(features: Array[CBufferFeature], supportBinary: Boolean): StructType = {
    StructType(
      features.map {
        case CBufferFeature(name, _, _, dataType: DataType, false) =>
          StructField(name, dataType = toSparkDataType(dataType))
        case CBufferFeature(name, _, arrayLength, DataType.Byte, true) =>
          if (supportBinary) StructField(name, dataType = BinaryType, metadata = new MetadataBuilder().putLong(ArrayLengthKey, arrayLength).build())
          else StructField(name, dataType = ArrayType(toSparkDataType(DataType.Byte)), metadata = new MetadataBuilder().putLong(ArrayLengthKey, arrayLength).build())
        case CBufferFeature(name, _, arrayLength, dataType: DataType, true) =>
          StructField(name, dataType = ArrayType(toSparkDataType(dataType)), metadata = new MetadataBuilder().putLong(ArrayLengthKey, arrayLength).build())
      })
  }

  /**
   * change column schema to indicate array length
   * usage one: withColumn("column name", indicateArrayLength("column name", arrayLength)
   * usage two: df.select(indicateArrayLength("column name", arrayLength), other columns)
   *
   * @param columnName  column name
   * @param arrayLength array length to indicate
   * @return
   */
  def indicateArrayLength(columnName: String, arrayLength: Int): Column = {
    val metadata = new MetadataBuilder().putLong(ArrayLengthKey, arrayLength).build()
    col(columnName).as(columnName, metadata = metadata)
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
    case BinaryType | ArrayType(ByteType, _) => (DataType.Byte, true)
    case ArrayType(elementType, _) =>
      if (topTile)
        (toInternalDataType(elementType, topTile = false)._1, true)
      else throw new UnsupportedOperationException(s"nested array is not supported")
    case _ => throw new UnsupportedOperationException(s"type ${dataType} is not supported")
  }

  // name, dataType, isArray, arrayLength
  def organizeFeatures(featureDefs: Array[(String, DataType, Boolean, Int)], columnBased: Boolean): Array[CBufferFeature] = {
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

        prevSize = CustomBufferDataGenerator.byteWidthOfArray(dataType, arrayLength, isArray && arrayLength == 0)
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
      .zipWithIndex
      .map {
        case (e, idx) => {
          nextStep(e._2, e._3, e._4)
          e._2 match {
            case DataType.Bool => CBufferFeature(e._1, if (columnBased) idx else index, offset, DataType.Bool, e._3)
            case _ => CBufferFeature(e._1, if (columnBased) idx else index, e._4, e._2, e._3)
          }
        }
      }
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

  final implicit class CBufferDataFrameWriter[T](writer: DataFrameWriter[T]) {
    def cb(path: String): Unit = {
      val df = readField[DataFrame]("df")
      val options = readField[CaseInsensitiveMap[String]]("extraOptions")
      this.writer
        .format("com.thetradedesk.featurestore.data.cbuffer.CBufferDataSource")
        .save(path)
      // update schema file
      val cBufferOptions = CBufferOptions(options + ("path" -> path))
      val features = SchemaHelper.inferFeature(df.schema, cBufferOptions.columnBased)
      CBufferDataSource.writeSchema(features, cBufferOptions)(df.sparkSession)
    }

    private def readField[U](name: String): U = {
      val field = classOf[DataFrameWriter[T]].getDeclaredField(name)
      field.setAccessible(true)
      field.get(writer).asInstanceOf[U]
    }
  }

  final implicit class CBufferDataFrameReader(reader: DataFrameReader) {
    def cb(path: String): DataFrame = {
      this.reader
        .format("com.thetradedesk.featurestore.data.cbuffer.CBufferDataSource")
        .load(path)
    }

    def cb(paths: String*): DataFrame = {
      this.reader
        .format("com.thetradedesk.featurestore.data.cbuffer.CBufferDataSource")
        .load(paths: _*)
    }
  }

  def internalRowToString(row: InternalRow, schema: StructType): String = {
    val values = schema.zipWithIndex.map { case (field, index) =>
      if (row.isNullAt(index)) {
        "null"
      } else {
        field.dataType match {
          case IntegerType => row.getInt(index).toString
          case LongType => row.getLong(index).toString
          case DoubleType => row.getDouble(index).toString
          case FloatType => row.getFloat(index).toString
          case BooleanType => row.getBoolean(index).toString
          case StringType => s""""${row.getUTF8String(index).toString}""""
          case BinaryType => s"[${row.getBinary(index).map(b => f"0x$b%02x").mkString(", ")}]"
          case DateType => row.getInt(index).toString + " (days since epoch)"
          case TimestampType => row.getLong(index).toString + " (microseconds since epoch)"
          case _ => row.get(index, field.dataType).toString
        }
      }
    }

    val fieldNames = schema.fieldNames
    val pairs = fieldNames.zip(values).map { case (name, value) => s"$name: $value" }
    s"InternalRow(${pairs.mkString(", ")})"
  }
}
