package com.thetradedesk.spark.sql

import org.apache.spark.sql.{Column, DataFrame, Dataset, Encoder}
import org.apache.spark.sql.functions._



object SQLFunctions {

  implicit class ColumnExtensions(val col: Column) {
    def isNullOrEmpty = isColumnNullOrEmpty(col)

    def isNotNullOrEmpty = !isColumnNullOrEmpty(col)
  }

  implicit class SymbolExtensions(val s: Symbol) {
    def isNullOrEmpty = isColumnNullOrEmpty(col(s.name))

    def isNotNullOrEmpty = !isColumnNullOrEmpty(col(s.name))
  }

  private def isColumnNullOrEmpty(col: Column) = col.isNull || col === ""

  implicit class DataFrameExtensions(val df: DataFrame) {
    /**
     * Combines `.select()` and `.as[U]()` in a single call.
     * Keeps all fields that are part of type U and drops all other fields.
     * @tparam U Target type for field selection
     * @return
     */
    def selectAs[U: Encoder]: Dataset[U] = {
      selectAs[U]()
    }

    /**
     * Combines `.select()` and `.as[U]()` in a single call.
     * Keeps all fields that are part of type U and drops all other fields.
     * @param nullIfAbsent true if fields of U not present in the dataset should be added as null columns, false if no fields should be added
     * @tparam U Target type for field selection
     * @return
     */
    def selectAs[U: Encoder](nullIfAbsent: Boolean = false): Dataset[U] = {
      val schema = implicitly[Encoder[U]].schema

      val fields = schema.map(s => s.name)
      df
        .transform(d => if (nullIfAbsent) {
          val fieldsInDf = d.schema.map(f => f.name).toSet
          val absentFields = schema.filter(f => !fieldsInDf.contains(f.name))
          // add column with null value for absent fields
          var out = d
          absentFields.foreach(f => out = out.withColumn(f.name, lit(null).cast(f.dataType)))
          out
        } else {
          d
        })
        // first select fields for this type
        .select(fields.head, fields.tail: _*)
        // and then transform it to the destination type
        .as[U]
    }
  }

}
