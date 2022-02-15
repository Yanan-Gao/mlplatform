package com.ttd.features.transformers

import com.github.mrpowers.spark.fast.tests.DatasetComparer
import com.ttd.ml.util.{TTDSparkTest, TempPathUtils}
import org.apache.spark.ml.Pipeline

class JoinTest extends TTDSparkTest with DatasetComparer {
  test("Serializable join stage") {
    val df = spark.createDataFrame(Seq(
      (1, Some(1)), (2, Some(2)), (1, Some(1)), (2, None)
    )).toDF("a", "b")

    // create input to join with
    df.createOrReplaceGlobalTempView("tempDF")

    TempPathUtils.withTempPath(path => {
      val pipe = new Pipeline().setStages(Array(
        Join("tempDF", "a", "inner"),
      ))

      val expected = pipe.fit(df).transform(df)

      pipe.write.overwrite().save(path)

      val actual = Pipeline.load(path).fit(df).transform(df)

      assertSmallDatasetEquality(expected, actual)
    })
  }
}
