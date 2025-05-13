package com.thetradedesk.featurestore.data.cbuffer

import org.apache.hadoop.conf.Configuration
import org.apache.spark.sql.execution.vectorized.WritableColumnVector

trait CBufferFileReader {

  def readBatch(num: Int, columnVectors: Array[WritableColumnVector]): Unit

  def prepareNextChunk(): Int
}
