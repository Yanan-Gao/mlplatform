package com.ttd.contextual.datasets.sources

import com.ttd.contextual.util.elDoradoUtilities.datasets.core.{GeneratedDataSet, S3Roots}
import com.ttd.contextual.util.elDoradoUtilities.datasets.core.{DatePartitionedS3DataSet, GeneratedDataSet, S3Roots}

case class WebScrapingData(version: Int = 1)
  extends DatePartitionedS3DataSet[WebScrapingDataRecord](
    GeneratedDataSet,
    S3Roots.DATAPIPELINE_SOURCES,
    s"/cxt_content/v=$version",
    // this needs to be disabled for large tables, the performance hit is too big
    mergeSchema = false
  ) {
  override protected val s3RootProd: String = normalizeUri(s"$s3Root")
  override protected val s3RootTest: String = normalizeUri(s"$s3Root")
}

case class WebScrapingDataLanguageTokenized(version: Int = 1)
  extends DatePartitionedS3DataSet[WebScrapingDataRecord](
    GeneratedDataSet,
    S3Roots.DATAPIPELINE_SOURCES,
    s"/cxt_tokenized_content/v=$version",
    // this needs to be disabled for large tables, the performance hit is too big
    mergeSchema = false
  ) {
  override protected val s3RootProd: String = normalizeUri(s"$s3Root")
  override protected val s3RootTest: String = normalizeUri(s"$s3Root")
}

case class WebScrapingDataRecord(
                                  UpdatedDateTimeUtc: java.sql.Timestamp,
                                  Url: String,
                                  OriginalUrl: String,
                                  Domain: String,
                                  TextContent: String,
                                  PageContent: String,
                                  TextContentWordCount: Int,
                                  TextContentCharacterCount: Int
                              )

case class WebScrapingDataLanguageTokenizedRecord(
                                  UpdatedDateTimeUtc: java.sql.Timestamp,
                                  Url: String,
                                  OriginalUrl: String,
                                  Language: String,
                                  Tokens: Seq[String],
                                  Lemmas: Seq[String],
                                  FilteredTokens: Seq[String],
                                  ExactMatchTokens: Seq[String]
                                )