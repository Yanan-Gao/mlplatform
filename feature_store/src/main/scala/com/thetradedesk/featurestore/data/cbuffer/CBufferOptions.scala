package com.thetradedesk.featurestore.data.cbuffer

import com.thetradedesk.featurestore.data.cbuffer.CBufferConstants._
import org.apache.parquet.compression.CompressionCodecFactory
import org.apache.spark.sql.catalyst.util.CaseInsensitiveMap

case class CBufferOptions(schemaPath: Option[String],
                          maxChunkRecordCount: Int,
                          defaultChunkRecordSize: Int,
                          defaultReachBatch: Int,
                          bigEndian: Boolean,
                          useOffHeap: Boolean,
                          outputPath: Option[String],
                          fixedChunkBuffer: Boolean,
                          encryptedMode: Boolean,
                          codecFactory: CompressionCodecFactory)

object CBufferOptions {
  def apply(parameters: CaseInsensitiveMap[String]): CBufferOptions = CBufferOptions(parameters.get(SchemaPathName),
    parameters.get(MaxChunkRecordCountKey).map(_.toInt).getOrElse(DefaultMaxChunkRecordCount),
    parameters.get(DefaultChunkRecordSizeKey).map(_.toInt).getOrElse(DefaultRecordSize),
    parameters.get(DefaultReadBatchKey).map(_.toInt).getOrElse(DefaultReadBatch),
    "true".equals(parameters.getOrElse(BigEndianKey, "false")),
    "true".equals(parameters.getOrElse(UseOffHeapKey, "false")),
    parameters.get("path"),
    "true".equals(parameters.getOrElse(FixedChunkBufferKey, "false")),
    false,
    null)

  def apply(parameters: Map[String, String]): CBufferOptions = this (CaseInsensitiveMap[String](parameters))
}
