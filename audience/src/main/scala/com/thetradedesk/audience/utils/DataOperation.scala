package com.thetradedesk.audience.utils

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._

object DataFrameUtils {

  def explodeColumns(df: DataFrame, columns: Seq[String]): DataFrame = {

    val schema = df.schema

    // Convert non-array columns to arrays
    val dfWithArrays = columns.foldLeft(df) { (tempDf, colName) =>
      if (schema(colName).dataType.isInstanceOf[ArrayType]) {
        tempDf
      } else {
        tempDf.withColumn(colName, array(col(colName)))
      }
    }

    val zippedExpr = arrays_zip(columns.map(col): _*)
    val dfZipped = dfWithArrays.withColumn("zipped", zippedExpr)
    val dfExploded = dfZipped.withColumn("exploded", explode(col("zipped")))

    // Extract values
    val dfWithCols = columns.foldLeft(dfExploded) { (tempDf, colName) =>
      tempDf.withColumn(colName, col(s"exploded.$colName"))
    }

    // Drop intermediate columns
    dfWithCols.drop("zipped", "exploded")
  }
}
