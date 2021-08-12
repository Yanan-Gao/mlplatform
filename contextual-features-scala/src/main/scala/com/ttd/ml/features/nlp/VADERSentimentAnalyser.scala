package com.ttd.ml.features.nlp

import org.apache.spark.ml.Transformer
import org.apache.spark.ml.param.{Param, ParamMap}
import org.apache.spark.ml.util.Identifiable
import org.apache.spark.sql.PythonRunnerWrapper.{PYTHON_EVAL_TYPE, mapInPandasBuilder}
import org.apache.spark.sql.execution.python.UserDefinedPythonFunction
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.apache.spark.sql.{DataFrame, Dataset}

/*
* VADER (Valence Aware Dictionary and sEntiment Reasoner) is a lexicon and rule-based sentiment analysis tool.
* It is specifically attuned to sentiments expressed in social media.
* It is fully open-sourced under the [MIT License]
* Returns Sentiment Analysis of Sentences normalized between -1 and 1
* */
class VADERSentimentAnalyser extends Transformer {
  override val uid: String = Identifiable.randomUID("VADERSentimentAnalyser")

  final val sentenceCol = new Param[String](this, "sentenceCol", "The input sentences")
  final val sentimentCol = new Param[String](this, "sentimentCol", "The output sentiments")

  def setSentenceCol(value: String): this.type = set(sentenceCol, value)
  setDefault(sentenceCol -> "sentences")
  def setSentimentCol(value: String): this.type = set(sentimentCol, value)
  setDefault(sentimentCol -> "sentiment")

  val udfString: String =
    s"""
       |from vaderSentiment.vaderSentiment import SentimentIntensityAnalyzer
       |
       |def analyse_sentiment(batches):
       |  analyzer = SentimentIntensityAnalyzer()
       |  for batch in batches:
       |    yield pd.Series([[analyzer.polarity_scores(s)['compound'] for s in sentences ] for sentences in batch])
       |""".stripMargin

  val udf: UserDefinedPythonFunction = mapInPandasBuilder(
    "analyse_sentiment",
    udfString,
    "ArrayType(FloatType())",
    ArrayType(FloatType),
    PYTHON_EVAL_TYPE.SQL_SCALAR_PANDAS_ITER_UDF
  )

  override def transform(ds: Dataset[_]): DataFrame = {
    ds
      .withColumn($(sentimentCol), udf(col($(sentenceCol))))
  }

  override def transformSchema(schema: StructType): StructType = {
    schema
        .add(StructField($(sentimentCol), ArrayType(FloatType), nullable = false))
  }

  override def copy(extra: ParamMap): VADERSentimentAnalyser = {
    defaultCopy(extra)
  }
}


