package com.thetradedesk.kongming.datasets

import com.thetradedesk.spark.sql.SQLFunctions._
import com.thetradedesk.spark.TTDSparkContext.spark
import com.thetradedesk.spark.TTDSparkContext.spark.implicits._
import org.apache.spark.sql.Dataset

final case class ContextualTaxonomyRecord(
                                           ContextualCategoryId: Long,
                                           // UniversalCategoryTaxonomyId: String,
                                           // ContextualStandardCategoryName: String,
                                           // Tier1: String,
                                           // Tier2: String,
                                           // Tier3: String,
                                           Tier1Id: Int
                                         )

object ContextualTaxonomyDataset {
  val S3Path: String = "s3://thetradedesk-useast-hadoop/Data_Science/Liu/TTD-contextual-standard-categories-extend.csv"

  def readHardCodedTier1Map(
                           ):Dataset[ContextualTaxonomyRecord]={
    val hardCodedTaxonomy = spark.read
      .options(Map("inferSchema" -> "true", "delimiter" -> ",", "header" -> "true"))
      .csv(s"${S3Path}")

    hardCodedTaxonomy.selectAs[ContextualTaxonomyRecord]
  }
}