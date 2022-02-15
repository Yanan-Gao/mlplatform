package com.ttd.contextual.features.keywords.history

import com.ttd.contextual.datasets.sources.ReadableDataFrame
import com.ttd.contextual.util.ReadableDataFrameUtils._
import com.ttd.contextual.util.TTDSparkTest
import com.ttd.features.visualise.PipelineAnalyser

case class History(url: String)

class KeywordHistoryTest extends TTDSparkTest {
  import spark.implicits._

  test("Keyphrase History run test") {
    val urls = spark.createDataFrame(Seq(
      ("1", Seq("a.com")),
      ("2", Seq("b.com")),
    )).toDF("uiid", "urlHistory")

    val keyphrases = spark.createDataFrame(Seq(
      ("a.com", Seq("a","b","c")),
    )).toDF("url", "keyphrases")

    val keyphraseConfig = new KeyphraseHistoryConfig {
      override val uiidCol: String = "uiid"
      override val urlHistoryCol: String = "urlHistory"
      override val keyphraseCol: String = "keyphrases"
      override val keyphraseUrlCol: String = "url"
      override val salt: Int = 5

      // Dataset config
      override val urlHistory: ReadableDataFrame = urls.asReadableDataFrame
      override val keyphraseContent: ReadableDataFrame = keyphrases.asReadableDataFrame
    }

    val keyphraseHistoryDriver = new KeyphraseHistoryDriver(keyphraseConfig)

    val result = keyphraseHistoryDriver
      .run
      .select("uiid", "keyphraseHistory")
      .as[(String, Seq[String])]
      .collect()

    val expected = Array(
      ("1", Seq("a","b","c"))
    )

    result should contain theSameElementsAs expected
  }

  test("Parameters are removed before join") {
    val urls = spark.createDataFrame(Seq(
      ("1", Seq("a.com/me?a=b")),
      ("2", Seq("b.com/me?b=a")),
    )).toDF("uiid", "urlHistory")

    val keyphrases = spark.createDataFrame(Seq(
      ("a.com/me?other=irrelevant", Seq("a", "b", "c")),
    )).toDF("url", "keyphrases")

    val keyphraseConfig = new KeyphraseHistoryConfig {
      override val uiidCol: String = "uiid"
      override val urlHistoryCol: String = "urlHistory"
      override val keyphraseCol: String = "keyphrases"
      override val keyphraseUrlCol: String = "url"

      // Dataset config
      override val urlHistory: ReadableDataFrame = urls.asReadableDataFrame
      override val keyphraseContent: ReadableDataFrame = keyphrases.asReadableDataFrame
    }

    val result = new KeyphraseHistoryDriver(keyphraseConfig)
      .run
      .select("uiid", "keyphraseHistory")
      .as[(String, Seq[String])]
      .collect()

    val expected = Array(
      ("1", Seq("a","b","c"))
    )

    result should contain theSameElementsAs expected
  }

  test("Bigram Histories") {
    val urls = spark.createDataFrame(Seq(
      ("1", Seq("a.com/me?a=b")),
      ("2", Seq("b.com/me?b=a")),
    )).toDF("uiid", "urlHistory")

    val keyphrases = spark.createDataFrame(Seq(
      ("a.com/me?other=irrelevant", Seq("a", "b", "c")),
    )).toDF("url", "keyphrases")

    val keyphraseConfig = new KeyphraseHistoryConfig {
      override val uiidCol: String = "uiid"
      override val urlHistoryCol: String = "urlHistory"
      override val keyphraseCol: String = "keyphrases"
      override val keyphraseUrlCol: String = "url"
      override val strategy = "bigram"

      // Dataset config
      override val urlHistory: ReadableDataFrame = urls.asReadableDataFrame
      override val keyphraseContent: ReadableDataFrame = keyphrases.asReadableDataFrame
    }

    val result = new KeyphraseHistoryDriver(keyphraseConfig)
      .run
      .select("uiid", "keyphraseHistory")
      .as[(String, Seq[String])]
      .collect()

    val expected = Array(
      ("1", Seq("a b","b c"))
    )

    result should contain theSameElementsAs expected
  }

  test("Yake Histories") {
    val urls = spark.createDataFrame(Seq(
      ("1", Seq("a.com/me?a=b")),
      ("2", Seq("b.com/me?b=a")),
    )).toDF("uiid", "urlHistory")

    val keyphrases = spark.createDataFrame(Seq(
      ("a.com/me?other=irrelevant",
        Seq("New", "in", "AI"))
    )).toDF("url", "keyphrases")

    val keyphraseConfig = new KeyphraseHistoryConfig {
      override val uiidCol: String = "uiid"
      override val urlHistoryCol: String = "urlHistory"
      override val keyphraseCol: String = "keyphrases"
      override val keyphraseUrlCol: String = "url"
      override val strategy = "yake"

      // Dataset config
      override val urlHistory: ReadableDataFrame = urls.asReadableDataFrame
      override val keyphraseContent: ReadableDataFrame = keyphrases.asReadableDataFrame
    }

    val result = new KeyphraseHistoryDriver(keyphraseConfig)
      .run
      .select("uiid", "keyphraseHistory")
      .as[(String, Seq[String])]
      .collect()

    val expected = Array(
      ("1", Seq("new", "ai", "new in ai"))
    )

    result should contain theSameElementsAs expected
  }

  test("Struct column name") {
    val urls = spark.createDataFrame(Seq(
      ("1", Seq(History("a.com/me?a=b"))),
      ("2", Seq(History("b.com/me?b=a"))),
    )).toDF("uiid", "history")

    val keyphrases = spark.createDataFrame(Seq(
      ("a.com/me?other=irrelevant", Seq("a", "b", "c")),
    )).toDF("url", "keyphrases")

    val keyphraseConfig = new KeyphraseHistoryConfig {
      override val uiidCol: String = "uiid"
      override val urlHistoryCol: String = "history.url"
      override val keyphraseCol: String = "keyphrases"
      override val keyphraseUrlCol: String = "url"

      // Dataset config
      override val urlHistory: ReadableDataFrame = urls.asReadableDataFrame
      override val keyphraseContent: ReadableDataFrame = keyphrases.asReadableDataFrame
    }

    val result = new KeyphraseHistoryDriver(keyphraseConfig)
      .run
      .select("uiid", "keyphraseHistory")
      .as[(String, Seq[String])]
      .collect()

    val expected = Array(
      ("1", Seq("a","b","c"))
    )

    result should contain theSameElementsAs expected
  }

  test("KeywordHistory visualisation") {
    val urls = spark.createDataFrame(Seq(
      ("1", Seq(History("a.com/me?a=b"))),
      ("2", Seq(History("b.com/me?b=a"))),
    )).toDF("uiid", "history")

    val keyphrases = spark.createDataFrame(Seq(
      ("a.com/me?other=irrelevant", Seq("a", "b", "c")),
    )).toDF("url", "keyphrases")

    val keyphraseConfig = new KeyphraseHistoryConfig {
      override val uiidCol: String = "uiid"
      override val urlHistoryCol: String = "history.url"
      override val keyphraseCol: String = "keyphrases"
      override val keyphraseUrlCol: String = "url"

      // Dataset config
      override val urlHistory: ReadableDataFrame = urls.asReadableDataFrame
      override val keyphraseContent: ReadableDataFrame = keyphrases.asReadableDataFrame
    }

    val pipe = new KeyphraseHistoryPipeline(keyphraseConfig)

    println(new PipelineAnalyser().explore(pipe))
  }
}
