package com.ttd.ml.pipelines

import com.ttd.ml.util.TTDSparkTest
import org.apache.spark.ml.Pipeline

class PythonSentimentPipelineTest extends TTDSparkTest {

  import spark.implicits._

  test("Python Sentiment matches expected sentiment") {
    val exampleTextDF = Seq(
      "Wow, I had a great time",
      "The pandemic is causing a lot of problems",
      "What an amazing goal",
      "I had a wonderful time. This conflicting sentence had a horrible time."
    ).toDF("text")

    val sentimentPipeline: Pipeline = PythonSentimentPipeline
        .VADERPipelineSentiment("text", "sentiment")

    val result = sentimentPipeline
      .fit(exampleTextDF)
      .transform(exampleTextDF)
      .select("sentiment")
      .as[Seq[Float]]
      .collect()

    // We expect that the sentiments should be positive (>0) or negative (<0) for the input sentences above:
    val expected = Seq(Seq("pos"), Seq("neg"), Seq("pos"), Seq("pos", "neg"))
    assert(expected.zip(result)
      .forall{case (es, scores) =>
        (es.length == scores.length) && es.zip(scores).forall{
          case ("pos", s) => s > 0
          case ("neg", s) => s < 0
          case _ => false
        }
      })
  }
}
