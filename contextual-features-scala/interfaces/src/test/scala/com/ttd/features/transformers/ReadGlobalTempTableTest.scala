package com.ttd.features.transformers

import com.github.mrpowers.spark.fast.tests.DatasetComparer
import com.ttd.ml.util.{TTDSparkTest, TempPathUtils}
import org.apache.spark.ml.Pipeline
import org.apache.spark.sql.functions.col

class ReadGlobalTempTableTest extends TTDSparkTest with DatasetComparer {
  test("Serializable read table stage") {
    val df = spark.createDataFrame(Seq(
      (1, Some(1)), (2, Some(2)), (1, Some(1)), (2, None)
    )).toDF("a", "b")
    df.createOrReplaceGlobalTempView("tempDF")

    TempPathUtils.withTempPath(path => {
      val pipe = new Pipeline().setStages(Array(
        ReadGlobalTempTable("tempDF")
      ))

      val emptyDF = spark.createDataFrame(Seq.empty[(Int, Int)])

      val expected = pipe.fit(df).transform(emptyDF)

      pipe.write.overwrite().save(path)

      val actual = Pipeline.load(path).fit(df).transform(emptyDF)

      assertSmallDatasetEquality(expected, actual)
    })
  }

  test("sql query plans are the same after read and select") {
    val df = spark.createDataFrame(Seq(
      (1, Some(1)), (2, Some(2)), (1, Some(1)), (2, None)
    )).toDF("a", "b")
    df.createOrReplaceGlobalTempView("tempDF")

    val pipe = new Pipeline().setStages(Array(
      WithColumn("c", col("a") * 100),
      CreateGlobalTempTable("furr"),
      ReadGlobalTempTable("furr"),
      Select("a", "c"),
      DropNA()
    ))


    df
      .withColumn("c", col("a") * 100)
      .select("a", "c")
      .na.drop()
      .explain(true)

    pipe.fit(df).transform(df).explain(true)
  }
}
