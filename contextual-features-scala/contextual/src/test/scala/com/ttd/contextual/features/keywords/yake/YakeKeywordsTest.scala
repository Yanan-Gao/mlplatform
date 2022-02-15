package com.ttd.contextual.features.keywords.yake

import com.ttd.contextual.datasets.sources.ReadableDataFrame
import com.ttd.contextual.util.ReadableDataFrameUtils._
import com.ttd.contextual.util.TTDSparkTest

class YakeKeywordsTest extends TTDSparkTest {
  import spark.implicits._

  test("YakeKeywordDriver run test") {
    val df = spark.createDataFrame(Seq(
      ("Flat earthers believe the world is not round.", "eng"),
      ("d e f", "spa"),
    )).toDF("text", "language")

    val yakeConfig: YakeDriverConfig = new YakeDriverConfig {
      override val textCol = "text"
      override val languageCol: String = "language"
      override val language: String = "eng"
      override val textContent: ReadableDataFrame = df.asReadableDataFrame
      override val minNGrams: Int = 3
      override val maxNGrams: Int = 3
      override val finalCols: Seq[String] = Seq("language", "keyphrases")
      override val minDocumentCharLength: Int = 0
      override val numPartitions = 1
    }

    val yakeDriver = new YakeKeyword(yakeConfig)

    val result = yakeDriver
      .run
      .as[(String, Seq[String])]
      .collect()

    val expected = Array(
      ("eng", Seq("flat earthers believe", "believe the world"))
    )

    result should contain theSameElementsAs expected
  }
}
