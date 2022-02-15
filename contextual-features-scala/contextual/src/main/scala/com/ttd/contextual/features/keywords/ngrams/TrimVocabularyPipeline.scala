package com.ttd.contextual.features.keywords.ngrams

import com.ttd.features.transformers._
import org.apache.spark.ml.Pipeline
import org.apache.spark.sql.functions._

// Trims keywords to by their count in a corpus and keeps only those that:
// 1. appeared the top `trimBelow` amount (if appeared too little then remove as regularisation)
// 2. appeared below the top `trimAbove` (if appeared too often then treated as stopword)
class TrimVocabularyPipeline(trimBelow: Int, trimAbove: Int, urlCol: String, tokenCol: String) extends Pipeline {
  assert(trimBelow >= trimAbove, "trimBelow is not greater than trimAbove")
  setStages(Array(
    Select(Seq(col(urlCol), explode(col(tokenCol)) as "rawTokens")),
    CreateGlobalTempTable("originalInputTable"),
    Agg(
      groupBy = Seq(col("rawTokens")),
      aggCols = Seq(count("*") as "count")
    ),
    WithWindowColumn("rank",
      column = row_number(),
      partitionBy = Seq(),
      orderBy = Seq(-col("count"))
    ),
    Filter((col("rank") >= trimAbove) && (col("rank") <= trimBelow)),
    Select(Seq(col("rawTokens"))),
    Join(
      "originalInputTable", "rawTokens", "inner"
    ),
    Agg(
      groupBy = Seq(col(urlCol)),
      aggCols = Seq(collect_list("rawTokens") as tokenCol)
    ),
  ))
}
