package com.thetradedesk.featurestore.udfs

import org.apache.spark.sql.functions.udf

object BinaryOperations {
  val binaryToLongArrayUdf = udf({ data: Array[Byte] =>
    data.map(_.longValue()).toArray
  })
}