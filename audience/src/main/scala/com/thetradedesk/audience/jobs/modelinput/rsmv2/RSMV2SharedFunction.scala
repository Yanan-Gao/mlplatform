package com.thetradedesk.audience.jobs.modelinput.rsmv2

import com.thetradedesk.audience.date
import com.thetradedesk.spark.TTDSparkContext.spark
import com.thetradedesk.spark.util.io.FSUtils
import org.apache.spark.sql.{DataFrame, Column}
import org.apache.spark.sql.functions.udf
import com.thetradedesk.featurestore.data.cbuffer.SchemaHelper.{CBufferDataFrameReader, CBufferDataFrameWriter, indicateArrayLength}
import java.time.format.DateTimeFormatter
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._

object RSMV2SharedFunction {
  def getDateStr(pattern : String = "yyyyMMdd") = {
    val dateFormatter = DateTimeFormatter.ofPattern(pattern)
    date.format(dateFormatter)
  }

  def writeOrCache(writePathIfNeed : Option[String], overrideMode: Boolean, dataset: DataFrame, cache: Boolean = true): DataFrame = {
    if (writePathIfNeed.isEmpty) {
      return if(cache) dataset.cache() else dataset
    }
    val writePath = writePathIfNeed.get
    if (overrideMode || !FSUtils.fileExists(writePath + "/_SUCCESS")(spark)) {
      dataset.write.mode("overwrite").parquet(writePath)
    }
    spark.read.parquet(writePath)
  }

  def castDoublesToFloat(df: DataFrame): DataFrame = {
    val cols: Seq[Column] = df.schema.fields.map { f =>
      f.dataType match {

        case DoubleType =>
          col(f.name).cast(FloatType).alias(f.name)

        case ArrayType(DoubleType, containsNull) =>
          transform(col(f.name), x => x.cast(FloatType)).alias(f.name)

        case _ =>
          col(f.name)
      }
    }

    df.select(cols: _*)
  }

  def paddingColumns(
      df: DataFrame,
      cols: Seq[String],
      padValue: Any
  ): DataFrame = {

    val maxLens: Map[String, Int] = {
      val maxExprs: Seq[Column] = cols.map { c =>
        max(coalesce(size(col(c)), lit(0))).alias(c)
      }
      val row = df.agg(maxExprs.head, maxExprs.tail: _*).collect().head
      cols.map(c => c -> row.getAs[Int](c)).toMap
    }

    cols.foldLeft(df) { (tmp, c) =>
      val maxLength = maxLens(c)

      val ArrayType(elemType, _) =
        tmp.schema(c).dataType.asInstanceOf[ArrayType]

      // pad value can't be null for cbuffer data
      val padLit: Column = elemType match {
        case FloatType   => lit(padValue.toString.toFloat)
        case DoubleType  => lit(padValue.toString.toDouble)
        case IntegerType => lit(padValue.toString.toInt)
        case LongType    => lit(padValue.toString.toLong)
        case StringType  => lit(padValue.toString)
        case other       => lit(padValue).cast(other)
      }

      val emptyArr = array_repeat(lit(null).cast(elemType), 0)
      val padArr   = array_repeat(padLit, maxLength)

      tmp.withColumn(
        c,
        slice(
          concat(
            coalesce(col(c), emptyArr),
            padArr
          ),
          1,
          maxLength
        )
      ).withColumn(c, indicateArrayLength(c, maxLength)) 
      // cbuffer needs fixed array cols to indicate array length, otherwise it will have errors when reading
    }
  }
  
  val seedIdToSyntheticIdMapping =
    (mapping: Map[String, Int]) =>
      udf((origin: Array[String]) => {
        if (origin == null) Array.empty[Int]
        else origin.map(mapping.getOrElse(_, -1)).filter(_ >= 0)
      })

  object SubFolder extends Enumeration {
    type SubFolder = Value
    val Val, Holdout, Train = Value
  }

}
