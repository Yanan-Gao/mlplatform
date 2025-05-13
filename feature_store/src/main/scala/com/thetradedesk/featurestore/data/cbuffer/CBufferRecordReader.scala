package com.thetradedesk.featurestore.data.cbuffer

import com.thetradedesk.featurestore.data.generators.Feature
import org.apache.hadoop.fs.Path
import org.apache.hadoop.mapred.FileSplit
import org.apache.hadoop.mapreduce.{InputSplit, RecordReader, TaskAttemptContext}
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.execution.vectorized.{ColumnVectorUtils, OffHeapColumnVector, OnHeapColumnVector, WritableColumnVector}
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.vectorized.{ColumnVector, ColumnarBatch}

class CBufferRecordReader(options: CBufferOptions, schema: StructType, features: Array[CBufferFeature], capacity: Int) extends RecordReader[Void, InternalRow] {
  private var file: Path = _
  private var reader: CBufferFileReaderFacade = _
  private var rowsReturned: Int = 0
  private var totalRowCount: Int = 0

  private var batchIdx = 0
  private var numBatched = 0
  private var totalCountLoadedSoFar = 0

  private var columnarBatch: ColumnarBatch = null

  private var columnVectors: Array[WritableColumnVector] = null

  /**
   * If true, this class returns batches instead of rows.
   */
  private var returnColumnarBatch = false

  override def initialize(inputSplit: InputSplit, taskAttemptContext: TaskAttemptContext): Unit = {
    val configuration = taskAttemptContext.getConfiguration
    val split = inputSplit.asInstanceOf[FileSplit]
    this.file = split.getPath

    this.reader = new CBufferFileReaderFacade(this.file, this.features, this.options)

    this.totalRowCount = this.reader.openFile(configuration)
  }

  override def nextKeyValue(): Boolean = {
    if (this.columnarBatch == null) initBatch()

    if (this.returnColumnarBatch) return nextBatch()

    if (this.batchIdx >= this.numBatched) if (!nextBatch()) return false

    this.batchIdx += 1
    true
  }

  def initBatch(): Unit = initBatch(null, null)

  def initBatch(partitionColumns: StructType, partitionValues: InternalRow): Unit = {
    var batchSchema = new StructType
    for (f <- this.schema.fields) {
      batchSchema = batchSchema.add(f)
    }

    if (partitionColumns != null) {
      for (f <- partitionColumns.fields) {
        batchSchema = batchSchema.add(f)
      }
    }

    if (this.options.useOffHeap) {
      this.columnVectors = OffHeapColumnVector.allocateColumns(capacity, batchSchema).asInstanceOf[Array[WritableColumnVector]]
    }
    else {
      this.columnVectors = OnHeapColumnVector.allocateColumns(capacity, batchSchema).asInstanceOf[Array[WritableColumnVector]]
    }
    this.columnarBatch = new ColumnarBatch(this.columnVectors.asInstanceOf[Array[ColumnVector]])

    // partition columns from path
    if (partitionColumns != null) {
      val partitionIdx = this.schema.fields.length
      for (i <- partitionColumns.fields.indices) {
        ColumnVectorUtils.populate(this.columnVectors(i + partitionIdx), partitionValues, i)
        this.columnVectors(i + partitionIdx).setIsConstant()
      }
    }
  }

  private def nextBatch(): Boolean = {
    for (vector <- this.columnVectors) {
      vector.reset()
    }
    this.columnarBatch.setNumRows(0)
    if (this.rowsReturned >= this.totalRowCount) return false
    checkEndOfChunk()

    val num = Math.min(capacity.toLong, this.totalCountLoadedSoFar - this.rowsReturned).toInt

    this.reader.readBatch(num, this.columnVectors)

    this.rowsReturned += num
    this.columnarBatch.setNumRows(num)
    this.numBatched = num
    this.batchIdx = 0
    true
  }

  private def checkEndOfChunk(): Unit = {
    if (this.rowsReturned != this.totalCountLoadedSoFar) return
    this.totalCountLoadedSoFar += this.reader.prepareNextChunk()
  }

  override def getCurrentKey: Void = null

  override def getCurrentValue: InternalRow = {
    if (this.returnColumnarBatch) return this.columnarBatch.asInstanceOf[InternalRow]
    this.columnarBatch.getRow(this.batchIdx - 1)
  }

  override def getProgress: Float = this.rowsReturned.toFloat / this.totalRowCount

  def enableReturningBatches(): Unit = {
    this.returnColumnarBatch = true
  }

  override def close(): Unit = {
    if (this.columnarBatch != null) {
      this.columnarBatch.close()
      this.columnarBatch = null
    }

    if (this.reader != null) {
      this.reader.close()
      this.reader = null
    }
  }
}
