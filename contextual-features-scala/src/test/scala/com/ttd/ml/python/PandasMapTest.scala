package com.ttd.ml.python

import com.ttd.ml.util.TTDSparkTest
import org.apache.spark.sql.PythonRunnerWrapper
import org.apache.spark.sql.PythonRunnerWrapper.{PYTHON_EVAL_TYPE, mapInPandasExpression, mapInPandasBuilder}
import org.apache.spark.sql.catalyst.expressions.PythonUDF
import org.apache.spark.sql.execution.python.UserDefinedPythonFunction
import org.apache.spark.sql.types.{IntegerType, StructField, StructType}

class PandasMapTest extends TTDSparkTest {

  import spark.implicits._

  test("Map as pandas row by row in python") {
    val exampleTextDF = Seq(
      "one",
      "two two",
      "three three three"
    ).toDF("text")

    val udfString: String =
      s"""
         |def count_words(row):
         |  return len(row.split(" "))
         |""".stripMargin

    val udf: UserDefinedPythonFunction = mapInPandasBuilder(
      "count_words",
      udfString,
      "IntegerType()",
      IntegerType,
      PYTHON_EVAL_TYPE.SQL_BATCHED_UDF
    )

    val result = exampleTextDF
        .select(udf('text))
        .as[Int]
        .collect()
    val expected = Array(1,2,3)

    result should contain theSameElementsInOrderAs expected
  }

  test("Map as pandas batched Series in python") {
    val exampleTextDF = Seq(
      "one",
      "two two",
      "three three three"
    ).toDF("text")


    val udfString: String =
      s"""
         |def count_words(batch):
         |  return batch.str.split().str.len()
         |""".stripMargin

    val udf: UserDefinedPythonFunction = mapInPandasBuilder(
      "count_words",
      udfString,
      "IntegerType()",
      IntegerType,
      PYTHON_EVAL_TYPE.SQL_SCALAR_PANDAS_UDF
    )

    val result = exampleTextDF
      .select(udf('text))
      .as[Int]
      .collect()
    val expected = Array(1,2,3)

    result should contain theSameElementsInOrderAs expected
  }

  test("Iterable Map as pandas Series in python") {
    val exampleTextDF = Seq(
      "one",
      "two two",
      "three three three"
    ).toDF("text")


    val udfString: String =
      s"""
         |def count_words(batches):
         |  for batch in batches:
         |    yield batch.str.split().str.len()
         |""".stripMargin

    val udf: UserDefinedPythonFunction = mapInPandasBuilder(
      "count_words",
      udfString,
      "IntegerType()",
      IntegerType,
      PYTHON_EVAL_TYPE.SQL_SCALAR_PANDAS_ITER_UDF
    )

    val result = exampleTextDF
      .select(udf('text))
      .as[Int]
      .collect()
    val expected = Array(1,2,3)

    result should contain theSameElementsInOrderAs expected
  }

  test("Iterable Map as pandas Dataframe in python") {
    val exampleTextDF = Seq(
      "one",
      "two two",
      "three three three"
    ).toDF("text")


    val udfString: String =
      s"""
         |def count_words(batches):
         |  for batch in batches:
         |    yield batch["text"].str.split().str.len().to_frame(name="count")
         |""".stripMargin

    val udf: PythonUDF = mapInPandasExpression(
      "count_words",
      udfString,
      """StructType([StructField("count", IntegerType())])""",
      StructType(Array(StructField("count", IntegerType, nullable = false))),
      PYTHON_EVAL_TYPE.SQL_MAP_PANDAS_ITER_UDF,
      Seq($"text".expr)
    )

    val result = PythonRunnerWrapper.mapInPandas(exampleTextDF, udf)
      .as[Int]
      .collect()
    val expected = Array(1,2,3)

    result should contain theSameElementsInOrderAs expected
  }
}
