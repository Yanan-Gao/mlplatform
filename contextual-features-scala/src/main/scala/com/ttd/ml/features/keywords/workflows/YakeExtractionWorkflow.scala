package com.ttd.ml.features.keywords.workflows

import java.time.LocalDate
import java.time.format.DateTimeFormatter

import com.ttd.ml.datasets.generated.contextual.keywords.{Yake, YakeRecord}
import com.ttd.ml.datasets.sources.{WebScrapingData, WebScrapingDataLanguageTokenized}
import com.ttd.ml.util.elDoradoUtilities.spark.TTDConfig.config
import com.ttd.ml.pipelines.YakePipeline
import com.ttd.ml.spark.TTDSparkContext.spark.implicits._
import com.ttd.ml.util.functions.scoreMapperUDF
import org.apache.spark.sql.functions.substring


class YakeExtractionConfig {
  // parameters for all
  val ApplicationName = "YakeExtraction"
  val Environment: String = config.getString("ttd.env", "local")
  val date: LocalDate = config.getDate("date", LocalDate.now().minusDays(2))

  // Yake extraction workflow parameters
  val numberOfKeywords: Int = config.getInt("numberOfKeywords", 150)
  val minDocumentCharLength: Int = config.getInt("minDocumentCharLength", 50)
  val maxDocumentCharLength: Int = config.getInt("maxDocumentCharLength", 1000)

  // Parallelism parameters
  val fileCount: Int = config.getInt("fileCount", 100)
  val tempDir: String = config.getString("tempDir", "hdfs:///user/hadoop/output-temp-dir")
}

/**
 * Workflow for generating Yake Keyphrases from text content
 */
object YakeExtractionWorkflow {
  /**
   * Helper function to extract and sort scores from [[YakePipeline]]
   */
  val scoreMapper: (Seq[String], Seq[Map[String, String]]) => Seq[(String, Double)] =
    (phrases: Seq[String], scores: Seq[Map[String, String]]) => {
      phrases.zip(scores.map(_("score").toDouble)).sortBy(_._2).distinct
    }

  def run[T<:YakeExtractionConfig](config: T): Unit = {
    val formatter = DateTimeFormatter.ofPattern("yyyymmdd")
    val date = formatter.format(config.date)

    val yakePipeline = YakePipeline.yakePipeline("text", "keywords", config.numberOfKeywords)

    val englishUrls = WebScrapingDataLanguageTokenized()
      .readDate(date)
      .filter($"language" === "eng")
      .select("Url")
      .distinct()

    val englishWebData = WebScrapingData()
      .readDate(date)
      .select($"Url", $"TextContent" as "text", $"TextContentCharacterCount")
      .join(englishUrls, "Url")
      .filter('TextContentCharacterCount > config.minDocumentCharLength)
      .withColumn("text", substring('text, 1, config.maxDocumentCharLength))
      .repartition(6400)

    val ds =  yakePipeline
      .fit(englishWebData)
      .transform(englishWebData)
      .select('url, scoreMapperUDF($"keywords.result", $"keywords.metadata") as "keyphrases")
      .as[YakeRecord]

    Yake().writePartition(ds, config.date, config.fileCount, config.tempDir)
  }

  def main(args: Array[String]): Unit = {
    run(new YakeExtractionConfig)
  }
}
