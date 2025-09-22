package com.thetradedesk.philo.util

import org.apache.spark.sql.{DataFrame, Dataset}
import org.apache.spark.sql.functions.col

/**
 * Data transformation utilities for DataFrame operations.
 */
object DataUtils {

  /**
   * Flatten nested data structures in DataFrame columns.
   * For columns in flatten_set, extracts the 'value' field from nested structures.
   * 
   * @param data DataFrame to flatten
   * @param flatten_set Set of column names to flatten
   * @return DataFrame with flattened columns
   */
  def flattenData(data: DataFrame, flatten_set: Set[String]): DataFrame = {
    data.select(
      data.columns.map(
        c => if (flatten_set.contains(c))
          col(s"${c}.value").alias(c).alias(c)
        else col(c)
      ): _*
    )
  }

  /**
   * Add original (unhashed) columns to output data.
   * Creates new columns with "original" prefix for specified columns.
   * 
   * @param keptCols sequence of column names to add original versions for
   * @param data DataFrame to add columns to
   * @return tuple of (modified DataFrame, sequence of new column names)
   */
  def addOriginalCols(keptCols: Seq[String], data: DataFrame): (DataFrame, Seq[String]) = {
    // add unhashed columns to output data
    val newData = keptCols.foldLeft(data) { (tempDF, colName) =>
      tempDF.withColumn(s"original$colName", col(colName))
    }
    val newColNames = keptCols.map(colName => s"original$colName")
    (newData, newColNames)
  }

  /**
   * Debug utility to print dataset information.
   * Works with any Dataset[T] including DataFrame (which is Dataset[Row]).
   * 
   * @param varName name of the variable for logging
   * @param data Dataset to inspect
   */
  def debugInfo[T](varName: String, data: Dataset[T]): Unit = {
    println(s"$varName")
    println("---------------------------------------")
    data.printSchema()
    println("---------------------------------------")
  }
}