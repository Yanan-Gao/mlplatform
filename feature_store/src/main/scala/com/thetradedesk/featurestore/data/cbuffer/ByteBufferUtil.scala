package com.thetradedesk.featurestore.data.cbuffer

import java.nio.ByteBuffer

object ByteBufferUtil {
  def clean(byteBuffer: ByteBuffer): Unit = {
    if (byteBuffer.isDirect) {
      try {
        val cleanerMethod = classOf[ByteBuffer].getDeclaredMethod("cleaner")
        cleanerMethod.setAccessible(true)
        val cleaner = cleanerMethod.invoke(byteBuffer)
        val cleanMethod = cleaner.getClass.getDeclaredMethod("clean")
        cleanMethod.invoke(cleaner)
      } catch {
        case e: Exception => e.printStackTrace()
      }
    }
  }
}
