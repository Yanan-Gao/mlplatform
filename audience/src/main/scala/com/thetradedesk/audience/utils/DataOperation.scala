package com.thetradedesk.audience.utils

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions.{arrays_zip, col, explode}

object DataFrameUtils {

  def explodeColumns(df: DataFrame, columns: Seq[String]): DataFrame = {
    val zippedExpr = arrays_zip(columns.map(col): _*)
    val dfZipped = df.withColumn("zipped", zippedExpr)
    val dfExploded = dfZipped.withColumn("exploded", explode(col("zipped")))
    val dfWithCols = columns.foldLeft(dfExploded) { (tempDf, colName) =>
      tempDf.withColumn(colName, col(s"exploded.$colName"))
    }
    dfWithCols.drop("zipped", "exploded")
  }

}

