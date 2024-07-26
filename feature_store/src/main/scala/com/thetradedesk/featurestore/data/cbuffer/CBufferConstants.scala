package com.thetradedesk.featurestore.data.cbuffer

object CBufferConstants {
  val DefaultMaxChunkRecordCount = 1024
  val DefaultRecordSize = 1024
  val DefaultMaxRecordSize = 65535
  val DefaultReadBatch = 1024

  val BitsInInteger: Int = 4

  val ShortName = "cbuffer"
  val FileExtension = ".cb"
  val SchemaPathName = "schemaPath"
  val DefaultSchemaFileName = "_SCHEMA"
  val MaxChunkRecordCountKey = "maxChunkRecordCount"
  val DefaultChunkRecordSizeKey = "defaultChunkRecordSize"
  val DefaultReadBatchKey = "defaultReadBatch"
  val BigEndianKey = "bigEndian"
  val FixedChunkBufferKey = "fixedChunkBuffer"
  val UseOffHeapKey = "useOffHeap"
  val ArrayLengthKey = "arrayLength"
}
