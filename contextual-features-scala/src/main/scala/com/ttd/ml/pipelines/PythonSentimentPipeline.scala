package com.ttd.ml.pipelines

import com.johnsnowlabs.nlp.{DocumentAssembler, Finisher}
import com.johnsnowlabs.nlp.annotator.SentenceDetector
import com.ttd.ml.features.nlp.VADERSentimentAnalyser
import org.apache.spark.ml.Pipeline

/* Demo class that showcases python and scala in one sentiment pipeline */
object PythonSentimentPipeline {

  /** Creates a sentiment analyzer pipeline.
   *
   *  @param inputCol the input text column
   *  @param outputCol the output sentiment column
   *  @return A [[org.apache.spark.ml.Pipeline]] for analysing sentences of text
   */
  def VADERPipelineSentiment(inputCol: String, outputCol: String): Pipeline = {
    val document = new DocumentAssembler()
      .setInputCol(inputCol)
      .setOutputCol("document")

    val sentenceDetector = new SentenceDetector()
      .setInputCols("document")
      .setOutputCol("sentence")

    val finisher = new Finisher()
      .setInputCols("sentence")
      .setOutputCols("sentence")
      .setOutputAsArray(true)

    val sentimentDetector = new VADERSentimentAnalyser()
      .setSentenceCol("sentence")
      .setSentimentCol(outputCol)

    new Pipeline().setStages(
      Array(
        document,
        sentenceDetector,
        finisher,
        sentimentDetector
      )
    )
  }
}
