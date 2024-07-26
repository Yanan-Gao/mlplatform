package com.thetradedesk.featurestore.data.cbuffer

import java.nio.{ByteBuffer, ByteOrder}

object MemoryHelper {
  def allocateBuffer(capacity: Int, useOffHeap: Boolean, bigEndian: Boolean): ByteBuffer = {
    val byteOrder = if (bigEndian) ByteOrder.BIG_ENDIAN else ByteOrder.LITTLE_ENDIAN
    if (useOffHeap) {
      ByteBuffer.allocateDirect(capacity).order(byteOrder)
    } else {
      ByteBuffer.allocate(capacity).order(byteOrder)
    }
  }

  def nextAllocateSize(size: Int): Int = {
    if ((size & (size - 1)) == 0) {
      size
    } else {
      Integer.highestOneBit(size) << 1
    }
  }
}
