package com.ttd.features.transformers

import com.github.mrpowers.spark.fast.tests.DatasetComparer
import com.ttd.ml.util.{TTDSparkTest, TempPathUtils}
import org.apache.spark.ml.Pipeline
import org.apache.spark.sql.functions.row_number

class WithWindowColumnTest extends TTDSparkTest with DatasetComparer {
  import spark.implicits._

  test("Serializable with window column stages") {
    val df = spark.createDataFrame(Seq(
      (1, Some(1)), (2, Some(2)), (1, Some(1)), (2, None)
    )).toDF("a", "b")

    TempPathUtils.withTempPath(path => {
      val pipe = new Pipeline().setStages(Array(
        WithWindowColumn(
          colName = "rank",
          column = row_number(),
          partitionBy = Seq('a),
          orderBy = Seq('a, 'b)
        )
      ))

      val expected = pipe.fit(df).transform(df)

      pipe.write.overwrite().save(path)

      val actual = Pipeline.load(path).fit(df).transform(df)

      assertSmallDatasetEquality(expected, actual)
    })
  }
}
