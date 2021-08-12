package com.ttd.ml.pipelines

import com.ttd.ml.util.TTDSparkTest

class YakePipelineTest extends TTDSparkTest {

  import spark.implicits._

  test("Top keyphrases extracted from a sentence contain some expected keyphrases") {
    val exampleTextDF = Seq(
      "New in AI: A feature store that outperformed the competition has been developed with outstanding skill " +
        "and extreme amounts of humility."
    ).toDF("text")

    val yakePipeline = YakePipeline
      .yakePipeline("text", "keywords", nKeywords = 10, minNGrams = 2)

    val result = yakePipeline
      .fit(exampleTextDF)
      .transform(exampleTextDF)
      .select($"keywords.result")
      .as[Seq[String]]
      .collect()
      .flatten

    // We expect that these phrases should be in the top 10 keyphrases
    val expected = Seq("feature store", "outstanding skill", "outperformed the competition", "new in ai")

    result should contain allElementsOf expected
  }
}
