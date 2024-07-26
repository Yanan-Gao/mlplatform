package com.thetradedesk.featurestore.data.cbuffer

import com.thetradedesk.featurestore.configs.DataType
import upickle.default._

case class CBufferFeature(
                           name: String,
                           // start index in the byte array
                           index: Int,
                           // used as offset in the byte array for boolean types
                           // used as length of fixed array values
                           // default is 0
                           offset: Int,
                           dataType: DataType,
                           isArray: Boolean = false
                         )

object CBufferFeature {
  implicit val featureRW: ReadWriter[CBufferFeature] = macroRW[CBufferFeature]
}