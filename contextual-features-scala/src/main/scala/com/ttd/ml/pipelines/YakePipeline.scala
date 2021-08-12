package com.ttd.ml.pipelines

import com.johnsnowlabs.nlp.DocumentAssembler
import com.johnsnowlabs.nlp.annotator.{SentenceDetector, Tokenizer}
import com.johnsnowlabs.nlp.annotators.keyword.yake.YakeModel
import org.apache.spark.ml.Pipeline

/** Factory for JohnSnowLabs Yake extraction pipelines. */
object YakePipeline {
  /** Creates a yake extraction pipeline.
   *
   *  @param inputCol the input text column
   *  @param outputCol the output column name
   *  @param nKeywords the maximum number of yake keywords to be returned
   *  @return A [[org.apache.spark.ml.Pipeline]] for embedding multilingual text
   */
  def yakePipeline(inputCol: String,
                   outputCol: String,
                   nKeywords: Int,
                   minNGrams: Int = 1,
                   maxNGrams: Int = 3): Pipeline = {
    val document = new DocumentAssembler()
      .setInputCol(inputCol)
      .setOutputCol("document")

    val sentenceDetector = new SentenceDetector()
      .setInputCols("document")
      .setOutputCol("sentence")

    val tokenDetector = new Tokenizer()
      .setInputCols("sentence")
      .setOutputCol("token")

    val ym = new YakeModel()
      .setMinNGrams(minNGrams)
      .setMaxNGrams(maxNGrams)
      .setNKeywords(nKeywords)
      .setInputCols("token")
      .setOutputCol(outputCol)

    new Pipeline().setStages(
      Array(
        document,
        sentenceDetector,
        tokenDetector,
        ym
      )
    )
  }
}
