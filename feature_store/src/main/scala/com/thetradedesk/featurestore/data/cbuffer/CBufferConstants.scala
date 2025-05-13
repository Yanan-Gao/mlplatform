package com.thetradedesk.featurestore.data.cbuffer

import java.nio.charset.StandardCharsets

object CBufferConstants {
  val DefaultMaxChunkRecordCount = 1024
  val DefaultRecordSize = 1024
  val DefaultMaxRecordSize = 65535
  val DefaultReadBatch = 1024
  val DefaultVarColumnScaleRatio = 3
  val DefaultMaxFileSize : Long = Int.MaxValue

  val BitsInInteger: Int = 4

  val ShortName = "cbuffer"
  val FileExtension = ".cb"
  val SchemaPathName = "schemaPath"
  val DefaultSchemaFileName = "_SCHEMA"
  val MaxChunkRecordCountKey = "maxChunkRecordCount"
  val DefaultChunkRecordSizeKey = "defaultChunkRecordSize"
  val DefaultMaxFileSizeKey = "maxFileSize"
  val DefaultVarColumnScaleRatioKey = "defaultVarColumnScaleRatio"
  val DefaultReadBatchKey = "defaultReadBatch"
  val BigEndianKey = "bigEndian"
  val FixedChunkBufferKey = "fixedChunkBuffer"
  val ColumnBasedKey = "columnBasedChunk"
  val UseOffHeapKey = "useOffHeap"
  val ArrayLengthKey = "arrayLength"

  val MAGIC_STR: String = "CBU1"
  val MAGIC: Array[Byte] = MAGIC_STR.getBytes(StandardCharsets.US_ASCII)
  val COLUMN_MAGIC_STR: String = "CBU2"
  val COLUMN_MAGIC: Array[Byte] = COLUMN_MAGIC_STR.getBytes(StandardCharsets.US_ASCII)
  val EF_MAGIC_STR = "CBUE"
  val EFMAGIC: Array[Byte] = EF_MAGIC_STR.getBytes(StandardCharsets.US_ASCII)
  val INT_BYTES_LENGTH = 4

  val DataStart = 0
  val ChunkDataSizeOffset = 4
  val ChunkDataOffset = 8
}
