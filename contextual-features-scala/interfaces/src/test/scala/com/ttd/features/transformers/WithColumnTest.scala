package com.ttd.features.transformers

import com.github.mrpowers.spark.fast.tests.DatasetComparer
import com.ttd.ml.util.{TTDSparkTest, TempPathUtils}
import org.apache.spark.ml.Pipeline
import org.apache.spark.sql.functions.col

class WithColumnTest extends TTDSparkTest with DatasetComparer {
  test("Serializable with column stages") {
    val df = spark.createDataFrame(Seq(
      (1, Some(1)), (2, Some(2)), (1, Some(1)), (2, None)
    )).toDF("a", "b")

    TempPathUtils.withTempPath(path => {
      val pipe = new Pipeline().setStages(Array(
        WithColumn("c", col("a") * 3)
      ))

      val expected = pipe.fit(df).transform(df)

      pipe.write.overwrite().save(path)

      val actual = Pipeline.load(path).fit(df).transform(df)

      assertSmallDatasetEquality(expected, actual)
    })
  }
}
