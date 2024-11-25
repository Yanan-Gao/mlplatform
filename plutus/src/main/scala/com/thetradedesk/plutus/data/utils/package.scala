package com.thetradedesk.plutus.data

import org.apache.spark.sql.{DataFrame, DataFrameWriter, Row}
import org.joda.time.format.DateTimeFormat

import java.nio.{ByteBuffer, ByteOrder}
import java.time.format.DateTimeFormatter
import java.time.{LocalDateTime, ZoneId, ZoneOffset}
import java.util.UUID
import scala.concurrent.{ExecutionContext, Future}

package object utils {
  /*
      Convert two longs to UUID. Some byte swapping is needed to get the right value. This is due to some crazy endianness used in .net.
     */
  def uuidFromLongs(lo: Long, hi: Long): String = {
    val lo_buffer: ByteBuffer = ByteBuffer.allocate(8).order(ByteOrder.LITTLE_ENDIAN)
    lo_buffer.putLong(lo)

    val hi_buffer: ByteBuffer = ByteBuffer.allocate(8).order(ByteOrder.LITTLE_ENDIAN)
    hi_buffer.putLong(hi)

    val guidBytes: Array[Byte] = new Array[Byte](16)
    guidBytes(0) = lo_buffer.get(3)
    guidBytes(1) = lo_buffer.get(2)
    guidBytes(2) = lo_buffer.get(1)
    guidBytes(3) = lo_buffer.get(0)
    guidBytes(4) = lo_buffer.get(5)
    guidBytes(5) = lo_buffer.get(4)
    guidBytes(6) = lo_buffer.get(7)
    guidBytes(7) = lo_buffer.get(6)

    guidBytes(8) = hi_buffer.get(0)
    guidBytes(9) = hi_buffer.get(1)
    guidBytes(10) = hi_buffer.get(2)
    guidBytes(11) = hi_buffer.get(3)
    guidBytes(12) = hi_buffer.get(4)
    guidBytes(13) = hi_buffer.get(5)
    guidBytes(14) = hi_buffer.get(6)
    guidBytes(15) = hi_buffer.get(7)

    val bb: ByteBuffer = ByteBuffer.wrap(guidBytes)
    val high: Long = bb.getLong
    val low: Long = bb.getLong

    new UUID(high, low).toString
  }

  //  This codeblock helps converts dotnet ticks to millis
  //  (ref: From https://learn.microsoft.com/en-us/dotnet/api/system.datetime.ticks?view=net-7.0)
  //  10,000 tick = 1 ms (10,000,000 ticks = 1 second)
  val TICKS_PER_MILLISECOND = 10000
  //  dotnet ticks represent the elapsed time since 12 midnight, January 1, 0001
  //  whereas scala epoch millis measure time since 12 midnight, January 1, 1970
  //  This provides the offset for that difference.
  val TICKS_BEFORE_EPOCH = 621355968000000000L
  val localDatetimeToTicks = (localDateTime: LocalDateTime) =>
    (localDateTime.toInstant(ZoneOffset.UTC).toEpochMilli * TICKS_PER_MILLISECOND) + TICKS_BEFORE_EPOCH

  val ISO_DATE_FORMAT  = "yyyy-MM-dd'T'HH:mm:ss"
  def javaToJoda(javaDatetime: LocalDateTime) = {
      val javaString = javaDatetime.format(DateTimeFormatter.ofPattern(ISO_DATE_FORMAT))
      org.joda.time.LocalDateTime.parse(javaString, DateTimeFormat.forPattern(ISO_DATE_FORMAT))
  }

  def jodaToJava(jodaDatetime: org.joda.time.LocalDateTime) = {
    val jodaString = jodaDatetime.toString(DateTimeFormat.forPattern(ISO_DATE_FORMAT))
    LocalDateTime.parse(jodaString, DateTimeFormatter.ofPattern(ISO_DATE_FORMAT))
  }

  class ParallelParquetWriter() {

    private case class WriteTask(writer: DataFrameWriter[Row], path: String)

    // List to store writing tasks
    private var tasks: List[WriteTask] = List();

    // Function to submit a single write task in parallel
    def EnqueueWrite(writer: DataFrameWriter[Row], path: String): Unit = {
      tasks = tasks :+ WriteTask(writer, path)
    }

    // Function to wait for all tasks to finish
    def WriteAll(): Unit = {
      tasks.par.foreach( task =>
        task.writer.parquet(task.path)
      )
    }
  }
}
