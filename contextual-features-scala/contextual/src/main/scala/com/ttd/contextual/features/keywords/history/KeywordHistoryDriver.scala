package com.ttd.contextual.features.keywords.history

import com.ttd.contextual.datasets.sources.{DataPipelineTokenizedContent, ReadableDataFrame, UrlHistory}
import com.ttd.contextual.features.keywords.ngrams.{FilterEmptyGrams, NGram}
import com.ttd.contextual.pipelines.YakePipeline
import com.ttd.contextual.util.elDoradoUtilities.spark.TTDConfig.config
import com.ttd.features.transformers._
import com.ttd.features.transformers.util.{NormalizeUrl, NormalizeUrlHistory}
import com.ttd.features.{Feature, FeatureConfig, FeatureDriver}
import org.apache.spark.ml.{Pipeline, PipelineModel, PipelineStage}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{DataFrame, SaveMode}
import org.joda.time.DateTime

object KeyphraseStrategy {
  def fromConfig(config: KeyphraseHistoryConfig): Pipeline = {
    config.strategy match {
      case "unigram" => new Pipeline().setStages(Array.empty[PipelineStage])
      case "bigram" => new Pipeline().setStages(Array(
        new NGram()
          .setGramCol(config.keyphraseCol)
          .setN(2)
      ))
      case "yake" => new Pipeline().setStages(Array(
        WithColumn(config.keyphraseCol, concat_ws(" ", col(config.keyphraseCol))),
        YakePipeline.yakePipeline(
          inputCol = config.keyphraseCol,
          outputCol = "yakeKeyphrases",
          nKeywords = config.keyphraseSlice / 3 // finds unigrams, bigrams and trigrams
        ),
        WithColumn(config.keyphraseCol, slice(col("yakeKeyphrases"), 1, config.keyphraseSlice))
      ))
      case _ => throw new RuntimeException(s"Unknown strategy ${config.strategy}")
    }
  }
}

// needs to be a class so that can be extended with other people's config
class KeyphraseHistoryConfig extends FeatureConfig {
  // Keyphrase history workflow parameters
  val uiidCol: String = config.getString("uiidCol", "uiid")
  val urlHistoryCol: String = config.getString("urlCol", "history.url")
  val keyphraseCol: String = config.getString("keyphraseCol", "keyphrases")
  val keyphraseUrlCol: String = config.getString("keyphraseUrlCol", "Url")
  val keyphraseSlice: Int = config.getInt("keyphraseSlice", 100)
  val languageCol: String = config.getString("languageCol", "language")
  val languages: Seq[String] = config.getStringSeq("languages", Seq())
  // Parallelism parameters
  val fileCount: Int = config.getInt("fileCount", 100)
  val salt: Int = config.getInt("salt", 20)

  // strategy parameters
  val strategy: String = config.getString("strategy", "unigram")
  val nGrams: Int = config.getInt("nGrams", 1)

  // Dataset config
  val keyphraseContent: ReadableDataFrame = DataPipelineTokenizedContent()
  val keyphraseContentDateRanges: Seq[DateTime] = config.getStringSeq("keyphraseDateRange",
    Seq()).map(DateTime.parse)
  val keyphraseTempTableName: String = "urlKeyphrases"

  val urlHistory: ReadableDataFrame = UrlHistory()
  val urlHistoryDate: DateTime = DateTime.parse(config.getString("urlDateRange", DateTime.now.toString))
  val normUrlHistoryTempTableName: String = "tempUrlHistory"

  override def sources: Map[String, Seq[DateTime]] = Map(
    keyphraseContent.basePath -> keyphraseContentDateRanges,
    urlHistory.basePath -> Seq(urlHistoryDate)
  )

  val saveMode: SaveMode = SaveMode.Overwrite
  val dateFormat: String = "yyyyMMdd"
  val outputDate: DateTime = DateTime.parse(config.getString("outputDate", urlHistoryDate.toString))
  val outputLocation: String = config.getString("outputLocation",
    s"s3://thetradedesk-useast-hadoop/Data_Science/christopher.hawkes/application/feature/keyword-history"
      + s"/strategy=$strategy"
      + s"/date=${outputDate.toString(dateFormat)}"
  )
}

class KeyphraseHistoryDriver(override val config: KeyphraseHistoryConfig) extends Feature[KeyphraseHistoryConfig] {
  override val featureName: String = "YakeKeyphrase"

  // Normalizes urls to increase join coverage
  // could also increase coverage by looking for url matches with shorter paths
  override def createPipeline: Pipeline = new KeyphraseHistoryPipeline(config)

  override def getPipelineInput: DataFrame = {
    // create temp view for keyphrase input
    config.keyphraseContent
      .read(config.keyphraseContentDateRanges)
      .createOrReplaceGlobalTempView(config.keyphraseTempTableName)

    config.urlHistory.read(Seq(config.urlHistoryDate))
  }

  /**
   * write pipeline output using api if possible
   */
  override def writePipelineOutput(dataFrame: DataFrame, pipelineModel: PipelineModel): Unit = {
    dataFrame.repartition(config.fileCount).write.mode(config.saveMode).parquet(config.outputLocation)
  }
}

object KeyphraseHistoryDriver extends FeatureDriver[KeyphraseHistoryDriver] {
  override val driver: KeyphraseHistoryDriver = new KeyphraseHistoryDriver(new KeyphraseHistoryConfig)
}

class KeyphraseHistoryPipeline(config: KeyphraseHistoryConfig) extends Pipeline {
  setStages(Array(
    new NormalizeUrlHistoryPipeline(config),
    new KeyphraseInputPipeline(config),
    SaltedJoin(config.normUrlHistoryTempTableName, "normUrl", "inner", config.salt),
    Agg(groupBy = Seq(col(config.uiidCol)),
      Seq(array_distinct(flatten(collect_list(col(config.keyphraseCol))))
        as "keyphraseHistory"))
  ))
}

class NormalizeUrlHistoryPipeline(config: KeyphraseHistoryConfig) extends Pipeline {
  setStages(Array(
    // if urls are from a struct column, extract urls before normalizing
    IF(config.urlHistoryCol.contains("."), WithColumn(config.urlHistoryCol, col(config.urlHistoryCol))),
    NormalizeUrlHistory(config.urlHistoryCol, "normUrl"),
    Select(Seq(col(config.uiidCol), explode(col("normUrl")) as "normUrl")),
    CreateGlobalTempTable(config.normUrlHistoryTempTableName)
  ))
}

class KeyphraseInputPipeline(config: KeyphraseHistoryConfig) extends Pipeline {
  setStages(Array(
    ReadGlobalTempTable(config.keyphraseTempTableName),

    // Filter to desired languages only
    IF(config.languages.nonEmpty, Filter(col(config.languageCol).isInCollection(config.languages))),

    // Filter empty keyphrases and crop history size
    DropNA(Array(config.keyphraseUrlCol, config.keyphraseCol)),
    new FilterEmptyGrams().setGramCol(config.keyphraseCol).setLength(config.keyphraseSlice),

    /**
     * Normalize urls TODO: encapsulate in transformer
     */
    NormalizeUrl(config.keyphraseUrlCol, "normUrl"),
    Select(Seq(col("normUrl"), col(config.keyphraseUrlCol) as "url", col(config.keyphraseCol))),
    WithWindowColumn("rank", row_number(),
      partitionBy = Seq(col("normUrl")),
      // order by shortest url first, then by largest content - want main site with most content
      orderBy = Seq(length(col("url")), -size(col(config.keyphraseCol)))),
    // keep the tokens of only 1 of the normalized urls
    Filter(col("rank") === 1),

    // calculate strategy
    KeyphraseStrategy.fromConfig(config),

    Select(Seq(col("normUrl"),  col(config.keyphraseCol)))
  ))
}
