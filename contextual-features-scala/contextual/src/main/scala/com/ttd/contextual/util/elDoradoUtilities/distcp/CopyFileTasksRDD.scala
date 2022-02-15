package com.ttd.contextual.util.elDoradoUtilities.distcp

/* NOTE:
 * This is a direct copy from the Identity teams neocortex library. At first chance, this should be replaced with
 * a dependency on the library in question.
 *
 * Created by Vardan Tovmasyan on 9/18/2018
 * Copyright (c) 2018 by theTradeDesk. All rights reserved.
 * Borrowed some spark code from:
 * https://github.com/apache/spark/blob/master/core/src/main/scala/org/apache/spark/rdd/ParallelCollectionRDD.scala
 * https://github.com/apache/spark/blob/master/core/src/main/scala/org/apache/spark/util/Utils.scala
 */

import java.io._

import org.apache.spark.rdd.RDD
import org.apache.spark._
import org.apache.spark.serializer.{DeserializationStream, JavaSerializer, SerializationStream, SerializerInstance}

import scala.collection.immutable.NumericRange
import scala.collection.mutable.ArrayBuffer
import scala.util.control.NonFatal

private case class CopyTask(src: String,
                            dest: String,
                            len: Long,
                            blockLocations: Seq[String]) extends Serializable {
  override def equals(obj: scala.Any): Boolean = src.equals(obj.asInstanceOf[CopyTask].src)
}

private class CopyTasksPartition(var rddId: Long,
                                 var slice: Int,
                                 var values: Seq[CopyTask]
                                ) extends Partition with Serializable {

  def iterator: Iterator[CopyTask] = values.iterator

  override def hashCode(): Int = (41 * (41 + rddId) + slice).toInt

  override def equals(other: Any): Boolean = other match {
    case that: CopyTasksPartition =>
      this.rddId == that.rddId && this.slice == that.slice
    case _ => false
  }

  override def index: Int = slice

  @throws(classOf[IOException])
  private def writeObject(out: ObjectOutputStream): Unit = tryOrIOException {
    val sfactory = SparkEnv.get.serializer
    sfactory match {
      case js: JavaSerializer => out.defaultWriteObject()
      case _ =>
        out.writeLong(rddId)
        out.writeInt(slice)

        val ser = sfactory.newInstance()
        serializeViaNestedStream(out, ser)(_.writeObject(values))
    }
  }

  @throws(classOf[IOException])
  private def readObject(in: ObjectInputStream): Unit = tryOrIOException {

    val sfactory = SparkEnv.get.serializer
    sfactory match {
      case js: JavaSerializer => in.defaultReadObject()
      case _ =>
        rddId = in.readLong()
        slice = in.readInt()

        val ser = sfactory.newInstance()
        deserializeViaNestedStream(in, ser)(ds => values = ds.readObject[Seq[CopyTask]]())
    }
  }

  def serializeViaNestedStream(os: OutputStream, ser: SerializerInstance)(
    f: SerializationStream => Unit): Unit = {
    val osWrapper = ser.serializeStream(new OutputStream {
      override def write(b: Int): Unit = os.write(b)
      override def write(b: Array[Byte], off: Int, len: Int): Unit = os.write(b, off, len)
    })
    try {
      f(osWrapper)
    } finally {
      osWrapper.close()
    }
  }

  def deserializeViaNestedStream(is: InputStream, ser: SerializerInstance)(
    f: DeserializationStream => Unit): Unit = {
    val isWrapper = ser.deserializeStream(new InputStream {
      override def read(): Int = is.read()
      override def read(b: Array[Byte], off: Int, len: Int): Int = is.read(b, off, len)
    })
    try {
      f(isWrapper)
    } finally {
      isWrapper.close()
    }
  }

  def tryOrIOException[T](block: => T): T = {
    try {
      block
    } catch {
      case e: IOException =>
        throw e
      case NonFatal(e) =>
        throw new IOException(e)
    }
  }
}

private class CopyFileTasksRDD (sc: SparkContext,
                                @transient private val data: Seq[CopyTask],
                                numSlices: Int,
                                locationMapper: (Int) => Seq[String]) extends RDD[CopyTask](sc, Nil) {

  override def getPartitions: Array[Partition] = {
    val slices = CopyFileTasksRDD.slice(data, numSlices).toArray
    slices.indices.map(i => new CopyTasksPartition(id, i, slices(i))).toArray
  }

  override def compute(s: Partition, context: TaskContext): Iterator[CopyTask] = {
    new InterruptibleIterator(context, s.asInstanceOf[CopyTasksPartition].iterator)
  }

  override def getPreferredLocations(s: Partition): Seq[String] = {
    locationMapper(s.index)
  }
}

private object CopyFileTasksRDD {
  /**
   * Slice a collection into numSlices sub-collections. One extra thing we do here is to treat Range
   * collections specially, encoding the slices as other Ranges to minimize memory cost. This makes
   * it efficient to run Spark over RDDs representing large sets of numbers. And if the collection
   * is an inclusive Range, we use inclusive range for the last slice.
   */
  def slice(seq: Seq[CopyTask], numSlices: Int): Seq[Seq[CopyTask]] = {
    if (numSlices < 1) {
      throw new IllegalArgumentException("Positive number of partitions required")
    }
    // Sequences need to be sliced at the same set of index positions for operations
    // like RDD.zip() to behave as expected
    def positions(length: Long, numSlices: Int): Iterator[(Int, Int)] = {
      (0 until numSlices).iterator.map { i =>
        val start = ((i * length) / numSlices).toInt
        val end = (((i + 1) * length) / numSlices).toInt
        (start, end)
      }
    }
    seq match {
      case r: Range =>
        positions(r.length, numSlices).zipWithIndex.map { case ((start, end), index) =>
          // If the range is inclusive, use inclusive range for the last slice
          if (r.isInclusive && index == numSlices - 1) {
            new Range.Inclusive(r.start + start * r.step, r.end, r.step)
          }
          else {
            new Range(r.start + start * r.step, r.start + end * r.step, r.step)
          }
        }.toSeq.asInstanceOf[Seq[Seq[CopyTask]]]
      case nr: NumericRange[CopyTask] =>
        // For ranges of Long, Double, BigInteger, etc
        val slices = new ArrayBuffer[Seq[CopyTask]](numSlices)
        var r = nr
        for ((start, end) <- positions(nr.length, numSlices)) {
          val sliceSize = end - start
          slices += r.take(sliceSize).asInstanceOf[Seq[CopyTask]]
          r = r.drop(sliceSize)
        }
        slices
      case _ =>
        val array = seq.toArray // To prevent O(n^2) operations for List etc
        positions(array.length, numSlices).map { case (start, end) =>
          array.slice(start, end).toSeq
        }.toSeq
    }
  }
}
