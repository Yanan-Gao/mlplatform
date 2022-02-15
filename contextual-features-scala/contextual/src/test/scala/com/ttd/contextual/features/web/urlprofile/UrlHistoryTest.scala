package com.ttd.contextual.features.web.urlprofile

import com.ttd.contextual.datasets.sources.ReadableDataFrame
import com.ttd.contextual.util.ReadableDataFrameUtils._
import com.ttd.contextual.util.TTDSparkTest
import com.ttd.features.visualise.PipelineAnalyser

class UrlHistoryTest extends TTDSparkTest {
  import spark.implicits._

  test("Histories are by UrlHistoryTransformer in descending order of count & recency") {
    val df = spark.createDataFrame(Seq(
      ("1", Seq("a.com"), 100L),
      ("1", Seq("b.com"), 200L),
      ("2", Seq("b.com"), 300L),
      ("2", Seq("b.com"), 400L),
      ("2", Seq("c.com"), 500L),
    )).toDF("uiid", "url", "ts")

    val urlHistoryConfig = new UrlHistoryDriverConfig {
      override val urlCol = "url"
      override val uiidCol: String = "uiid"
      override val tsCol: String = "ts"
      override val maxDailyHistory: Int = 3
      override val avails: ReadableDataFrame = df.asReadableDataFrame
    }

    val urlHistoryDriver = new UrlHistoryDriver(urlHistoryConfig)

    val result = urlHistoryDriver
      .run
      .as[(String, Seq[(Long, Long, String)])]
      .collect()

    val expected = Array(
      ("1", Seq((1, 200L, "b.com"), (1, 100L, "a.com"))),
      ("2", Seq((2, 400L, "b.com"), (1, 500L, "c.com")))
    )

    result should contain theSameElementsAs expected
  }

  test("Histories are trimmed by UrlHistoryTransformer") {
    val df = spark.createDataFrame(Seq(
      ("1", Seq("a.com"), 100L),
      ("1", Seq("b.com"), 200L),
      ("1", Seq("b.com"), 300L),
      ("1", Seq("b.com"), 400L),
      ("1", Seq("b.com"), 500L),
      ("1", Seq("c.com"), 900L),
    )).toDF("uiid", "url", "ts")

    val urlHistoryConfig = new UrlHistoryDriverConfig {
      override val urlCol = "url"
      override val uiidCol: String = "uiid"
      override val tsCol: String = "ts"
      override val maxDailyHistory: Int = 2
      override val avails: ReadableDataFrame = df.asReadableDataFrame
    }

    val urlHistoryDriver = new UrlHistoryDriver(urlHistoryConfig)

    val result = urlHistoryDriver
      .run
      .as[(String, Seq[(Long, Long, String)])]
      .collect()

    val expected = Array(
      ("1", Seq((4, 500L, "b.com"), (1, 900L, "c.com")))
    )

    result should contain theSameElementsAs expected
  }

  test("UrlHistoryDriver run test") {
    val df = spark.createDataFrame(Seq(
      ("1", Seq("a.com"), 100L),
      ("1", Seq("b.com"), 200L),
      ("1", Seq("b.com"), 300L),
      ("1", Seq("b.com"), 400L),
      ("1", Seq("b.com"), 500L),
      ("1", Seq("c.com"), 900L),
    )).toDF("uiid", "url", "ts")

    val urlHistoryConfig = new UrlHistoryDriverConfig {
      override val urlCol = "url"
      override val uiidCol: String = "uiid"
      override val tsCol: String = "ts"
      override val maxDailyHistory: Int = 2
      override val avails: ReadableDataFrame = df.asReadableDataFrame
    }

    val urlHistoryDriver = new UrlHistoryDriver(urlHistoryConfig)

    val result = urlHistoryDriver
      .run
      .as[(String, Seq[(Long, Long, String)])]
      .collect()

    val expected = Array(
      ("1", Seq((4, 500L, "b.com"), (1, 900L, "c.com")))
    )

    result should contain theSameElementsAs expected
  }

  test("UrlHistoryDriver visualize test") {
    val df = spark.createDataFrame(Seq(
      ("1", Seq("a.com"), 100L),
    )).toDF("uiid", "url", "ts")

    val urlHistoryConfig = new UrlHistoryDriverConfig {
      override val urlCol = "url"
      override val uiidCol: String = "uiid"
      override val tsCol: String = "ts"
      override val avails: ReadableDataFrame = df.asReadableDataFrame
    }

    val pipe = new UrlHistoryDriver(urlHistoryConfig).createPipeline

    println(new PipelineAnalyser().explore(pipe))
  }
}
