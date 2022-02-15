package com.ttd.features.parameters

import org.apache.spark.ml.param.{Param, Params}
import org.apache.spark.sql.types.{DataType, StructType}
import org.json4s.DefaultFormats

class StructTypeParameter(parent: Params, name: String, doc: String, isValid: StructType => Boolean)
  extends Param[StructType](parent, name, doc, isValid) {

  def this(parent: Params, name: String, doc: String) =
    this(parent, name, doc, StructTypeParameter.alwaysTrue)

  override def jsonEncode(value: StructType): String = {
    value.json
  }

  override def jsonDecode(json: String): StructType = {
    implicit val formats: DefaultFormats.type = DefaultFormats
    DataType.fromJson(json).asInstanceOf[StructType]
  }
}

object StructTypeParameter {
  def alwaysTrue: StructType => Boolean = (_: StructType) => true
}

