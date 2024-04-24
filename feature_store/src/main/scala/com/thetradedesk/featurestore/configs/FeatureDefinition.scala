package com.thetradedesk.featurestore.configs

import com.thetradedesk.featurestore.constants.FeatureConstants.{AlphaNumericRegex, MaxArrayLength}
import com.thetradedesk.featurestore.entities.Result
import upickle.default._

case class FeatureDefinition(
                              name: String,
                              dtype: DataType,
                              // 0 means not array type, 1 means var length array, others means fixed length array
                              // the value must be between 0 - [[com.thetradedesk.features.constants.FeatureConstants.MaxArrayLength]]
                              arrayLength: Int = 0,
                              keyType: DataType = DataType.None,
                              valueType: DataType = DataType.None,
                              defaultValue: String = ""
                            ) {
  lazy val validate: Result = {
    if (name.isEmpty || !name.matches(AlphaNumericRegex)) {
      Result.failed("feature name is invalid")
    } else if (arrayLength < 0 || arrayLength > MaxArrayLength) {
      Result.failed(s"feature $name must have a valid offset value")
    } else if (arrayLength > 1 && typedDefaultValue.asInstanceOf[Array[_]].length != arrayLength) { // fixed length array
      Result.failed(s"feature $name default value length doesn't match with arrayLength")
    } else if (dtype == null) {
      Result.failed(s"feature $name must define dtype")
    } else {
      Result.succeed()
    }
  }

  lazy val typedDefaultValue = (dtype, arrayLength) match {
    case (DataType.Bool, 0) => tryParse[Boolean](_.toBoolean, defaultValue, false)
    case (DataType.Byte, 0) => tryParse[Long](_.toByte.longValue(), defaultValue, 0)
    case (DataType.Short, 0) => tryParse[Long](_.toShort.longValue(), defaultValue, 0)
    case (DataType.Int, 0) => tryParse[Long](_.toInt.longValue(), defaultValue, 0)
    case (DataType.Long, 0) => tryParse[Long](_.toLong, defaultValue, 0)
    case (DataType.Float, 0) => tryParse[Double](_.toFloat.doubleValue(), defaultValue, 0d)
    case (DataType.Double, 0) => tryParse[Double](_.toDouble, defaultValue, 0d)
    case (DataType.String, 0) => defaultValue
    case (DataType.Byte, 1) => tryParse[Array[Long]](read[Array[Byte]](_).map(_.longValue()).array, defaultValue, Array.empty[Long])
    case (DataType.Short, 1) => tryParse[Array[Long]](read[Array[Short]](_).map(_.longValue()).array, defaultValue, Array.empty[Long])
    case (DataType.Int, 1) => tryParse[Array[Long]](read[Array[Int]](_).map(_.longValue()).array, defaultValue, Array.empty[Long])
    case (DataType.Long, 1) => tryParse[Array[Long]](read[Array[Long]](_), defaultValue, Array.empty[Long])
    case (DataType.Float, 1) => tryParse[Array[Double]](read[Array[Double]](_).map(_.doubleValue()), defaultValue, Array.empty[Double])
    case (DataType.Double, 1) => tryParse[Array[Double]](read[Array[Double]](_), defaultValue, Array.empty[Double])
    case (DataType.Byte, _) => tryParse[Array[Long]](read[Array[Byte]](_).map(_.longValue()).array, defaultValue, new Array[Long](arrayLength))
    case (DataType.Short, _) => tryParse[Array[Long]](read[Array[Short]](_).map(_.longValue()).array, defaultValue, new Array[Long](arrayLength))
    case (DataType.Int, _) => tryParse[Array[Long]](read[Array[Int]](_).map(_.longValue()).array, defaultValue, new Array[Long](arrayLength))
    case (DataType.Long, _) => tryParse[Array[Long]](read[Array[Long]](_), defaultValue, new Array[Long](arrayLength))
    case (DataType.Float, _) => tryParse[Array[Double]](read[Array[Double]](_).map(_.doubleValue()), defaultValue, new Array[Double](arrayLength))
    case (DataType.Double, _) => tryParse[Array[Double]](read[Array[Double]](_), defaultValue, new Array[Double](arrayLength))
    case _ => throw new UnsupportedOperationException(this.toString)
  }

  private def tryParse[T](fun: String => T, value: String, defaultVal: T) = {
    try {
      if (value.isEmpty) {
        defaultVal
      } else {
        fun(value)
      }
    } catch {
      case _: Throwable => defaultVal
    }
  }

  override def toString: String = {
    s"name=$name,dtype=$dtype,arrayLength=$arrayLength,default=$defaultValue"
  }
}

object FeatureDefinition {
  implicit val FeatureDefinitionRW: ReadWriter[FeatureDefinition] = macroRW[FeatureDefinition]
}

sealed case class DataType private(value: Int) {

  def >=(dataType: DataType) = value >= dataType.value

  def <=(dataType: DataType) = value <= dataType.value

  def <(dataType: DataType) = value < dataType.value

  def >(dataType: DataType) = value > dataType.value
}

object DataType {
  implicit val dataTypeRW: ReadWriter[DataType] = readwriter[Int].bimap[DataType](_.value, intToDataType)

  val None = DataType(0)

  val Bool = DataType(1)

  val Byte = DataType(2)

  val Short = DataType(3)

  val Int = DataType(4)

  val Long = DataType(5)

  val Float = DataType(6)

  val Double = DataType(7)

  val String = DataType(8)

  val Map = DataType(9)

  def intToDataType(value: Int) = {
    value match {
      case Bool.value => Bool
      case Byte.value => Byte
      case Short.value => Short
      case Int.value => Int
      case Long.value => Long
      case Float.value => Float
      case Double.value => Double
      case String.value => String
      case Map.value => Map
      case _ => None
    }
  }
}