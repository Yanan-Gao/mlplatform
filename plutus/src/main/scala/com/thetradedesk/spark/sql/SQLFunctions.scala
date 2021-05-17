package com.thetradedesk.spark.sql

import org.apache.spark.sql.Column
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


}
