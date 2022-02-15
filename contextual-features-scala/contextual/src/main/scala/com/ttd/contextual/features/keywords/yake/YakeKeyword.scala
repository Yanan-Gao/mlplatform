package com.ttd.contextual.features.keywords.yake

import com.ttd.features.transformers.{Filter, Repartition, Select, WithColumn}
import com.ttd.features.{Feature, FeatureConfig, FeatureDriver}
import com.ttd.contextual.datasets.sources.{DataPipelineContent, ReadableDataFrame}
import com.ttd.contextual.pipelines.YakePipeline
import com.ttd.contextual.util.StopwordRemoverLanguageUtil.fromIso3
import com.ttd.contextual.util.elDoradoUtilities.spark.TTDConfig.config
import org.apache.spark.ml.feature.StopWordsRemover
import org.apache.spark.ml.{Pipeline, PipelineModel}
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions.{col, length, lit, substring}
import org.joda.time.DateTime

// needs to be a class so that can be extended with other people's config
class YakeDriverConfig extends FeatureConfig {
  // parameters for all
  val ApplicationName = "YakeExtraction"
  val Environment: String = config.getString("ttd.env", "local")

  // Yake extraction workflow parameters
  val numberOfKeywords: Int = config.getInt("numberOfKeywords", 50)
  val minDocumentCharLength: Int = config.getInt("minDocumentCharLength", 20)
  val maxDocumentCharLength: Int = config.getInt("maxDocumentCharLength", 1000)
  val minNGrams: Int = config.getInt("minNGrams", 1)
  val maxNGrams: Int = config.getInt("maxNGrams", 3)
  val numPartitions: Int = config.getInt("numPartitions", 6400)

  val textCol: String = config.getString("textCol", "text")
  val languageCol: String = config.getString("languageCol", "language") //TextContentCharacterCount
  val language: String = config.getString("languages", "eng")
  val finalCols: Seq[String] = config.getStringSeq("finalCols", Seq("url", "keyphrases", "scrores", languageCol))

  // Parallelism parameters
  val fileCount: Int = config.getInt("fileCount", 100)
  val tempDir: String = config.getString("tempDir", "hdfs:///user/hadoop/output-temp-dir")

  val textContent: ReadableDataFrame = DataPipelineContent()
  val textContentDateRanges: Seq[DateTime] = config.getStringSeq("dateRange", Seq()).map(DateTime.parse)
  val outputLocation: String = config.getString("outputLocation",
    "s3://thetradedesk-useast-hadoop/Data_Science/christopher.hawkes/application/feature/yake-keywords")

  override def sources: Map[String, Seq[DateTime]] = Map(
    textContent.basePath -> textContentDateRanges
  )
}

class YakeKeyword(override val config: YakeDriverConfig) extends Feature[YakeDriverConfig] {
  override val featureName: String = "YakeKeyphrase"

  override def createPipeline: Pipeline = new YakeKeywordPipeline(config)

  override def getPipelineInput: DataFrame = {
    config.textContent.read(config.textContentDateRanges)
  }

  override def writePipelineOutput(dataFrame: DataFrame, pipelineModel: PipelineModel): Unit = {
    dataFrame.write.parquet(config.outputLocation)
  }
}

class YakeKeywordPipeline(config: YakeDriverConfig) extends Pipeline {
  setStages(Array(
    // optionally filter to required languages only
    Filter(col(config.languageCol) === lit(config.language)),
    Filter(length(col(config.textCol)) > config.minDocumentCharLength),
    WithColumn(config.textCol, substring(col(config.textCol), 1, config.maxDocumentCharLength)),
    Repartition(config.numPartitions),
    YakePipeline.yakePipeline(
      config.textCol, "keyphrases", config.numberOfKeywords,
      config.minNGrams, config.maxNGrams,
      StopWordsRemover.loadDefaultStopWords(fromIso3(config.language))),
    Select(config.finalCols.head, config.finalCols.tail:_*)
  ))
}

object YakeKeywordDriver extends FeatureDriver[YakeKeyword] {
  override val driver: YakeKeyword = new YakeKeyword(new YakeDriverConfig)
}