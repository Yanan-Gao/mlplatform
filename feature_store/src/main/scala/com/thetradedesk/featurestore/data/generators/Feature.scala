package com.thetradedesk.featurestore.data.generators

import com.thetradedesk.featurestore.configs.DataType
import upickle.default._

case class Feature(
                    name: String,
                    dataSourceName: String,
                    // start index in the byte array
                    index: Int,
                    // used as offset in the byte array for boolean types
                    // used as length of fixed array values
                    // default is 0
                    offset: Int,
                    dataType: DataType,
                    isArray: Boolean = false,
                    isLastFeature: Boolean = false
                  ) {
  lazy val fullName = s"${dataSourceName}_${name}"
}

object Feature {
  implicit val featureRW: ReadWriter[Feature] = macroRW[Feature]
}