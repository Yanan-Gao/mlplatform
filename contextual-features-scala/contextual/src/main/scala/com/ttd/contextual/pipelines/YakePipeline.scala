package com.ttd.contextual.pipelines

import com.johnsnowlabs.nlp.DocumentAssembler
import com.johnsnowlabs.nlp.annotator.{SentenceDetector, Tokenizer}
import com.johnsnowlabs.nlp.annotators.keyword.yake.YakeKeywordExtraction
import com.ttd.contextual.features.keywords.yake.YakeKeywordFinisher
import com.ttd.contextual.util.StopwordRemoverLanguageUtil.fromIso3
import org.apache.spark.ml.Pipeline
import org.apache.spark.ml.feature.{HashingTF, IDF, StopWordsRemover}

/** Factory for JohnSnowLabs Yake extraction pipelines. */
object YakePipeline {
  /** Creates a yake extraction pipeline.
   *
   *  @param inputCol the input text column
//   *  @param outputCol the output column name
   *  @param nKeywords the maximum number of yake keywords to be returned
   *  @return A [[org.apache.spark.ml.Pipeline]] for embedding multilingual text
   */
  def yakePipeline(inputCol: String,
                   outputCol: String = "keyphrases",
                   nKeywords: Int,
                   minNGrams: Int = 1,
                   maxNGrams: Int = 3,
                   stopwords: Array[String] = StopWordsRemover.loadDefaultStopWords(fromIso3("eng"))): Pipeline = {
    val document = new DocumentAssembler()
      .setInputCol(inputCol)
      .setOutputCol("document")

    val sentenceDetector = new SentenceDetector()
      .setInputCols("document")
      .setOutputCol("sentence")

    val tokenDetector = new Tokenizer()
      .setInputCols("sentence")
      .setOutputCol("token")
//      .setContextChars()

    val ym = new YakeKeywordExtraction()
      .setMinNGrams(minNGrams)
      .setMaxNGrams(maxNGrams)
      .setNKeywords(nKeywords)
      .setStopWords(stopwords)
      .setInputCols("token")
      .setOutputCol("yakeCol")

    val yakeFinisher = new YakeKeywordFinisher()
      .setYakeCol("yakeCol")
      .setKeyphraseCol(outputCol)
      .setScoresCol("scores")

    new Pipeline().setStages(
      Array(
        document,
        sentenceDetector,
        tokenDetector,
        ym,
        yakeFinisher
      )
    )
  }

  /** Creates a yake extraction pipeline.
   *
   *  @param inputCol the input tokenized text column
   *  @param outputCol the output column name
   *  @param hashSize the maximum number of yake keywords to be returned
   *  @return A [[org.apache.spark.ml.Pipeline]] for embedding multilingual text
   */
  def hashTFIDF(inputCol: String,
                     outputCol: String,
                     hashSize: Int = 262144): Pipeline = {
    val hashingTF = new HashingTF()
      .setInputCol(inputCol)
      .setOutputCol("rawFeatures")
      .setNumFeatures(hashSize)

    val idf = new IDF()
      .setInputCol("rawFeatures")
      .setOutputCol(outputCol)

    new Pipeline().setStages(
      Array(
        hashingTF,
        idf
      )
    )
  }
}