package com.thetradedesk.featurestore.utils

import com.thetradedesk.featurestore.entities.Result
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.types.{ArrayType, StructField, StructType}

object DatasetHelper {


  def setNullableState(df: DataFrame, nullable: Boolean = false, containsNull: Boolean = false): DataFrame = {
    val schema = df.schema
    val newSchema = StructType(schema.map {
      case StructField(c, t: ArrayType, _, m) => StructField(c, ArrayType(t.elementType, containsNull = containsNull), nullable = nullable, m)
      case StructField(c, t, _, m) => StructField(c, t, nullable = nullable, m)
    })

    df.sqlContext.createDataFrame(df.rdd, newSchema)
  }

  def compareSmallDatasets(actualDS: DataFrame, expectedDS: DataFrame): Result = {
    if (!SchemaComparer.equals(actualDS.schema, expectedDS.schema, false, false)) {
      Result.failed("schema mismatch")
    } else {
      val a = actualDS.collect()
      val e = expectedDS.collect()
      if (!a.sameElements(e)) {
        Result.failed("data mismatch")
      } else {
        Result.succeed()
      }
    }
  }

  def refineDataFrame(df: DataFrame, orderBy: String): DataFrame = {
    setNullableState(
      df
        .select(df.columns.sorted.map(col): _*)
        .orderBy(col(orderBy).asc)
    )
  }
}
