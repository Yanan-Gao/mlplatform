package com.thetradedesk.kongming.transform

import com.thetradedesk.spark.sql.SQLFunctions._
import com.thetradedesk.kongming.datasets._
import com.thetradedesk.spark.TTDSparkContext.spark
import org.apache.spark.sql.{DataFrame, Dataset}
import org.apache.spark.sql.functions._
import com.thetradedesk.spark.TTDSparkContext.spark.implicits._
import com.thetradedesk.spark.util.prometheus.PrometheusClient
import org.apache.spark.sql.expressions.{UserDefinedFunction, Window}

object ContextualTransform {
  val CARDINALITY_ALL_CATEGORY: Int = 700
  val CARDINALITY_TIER1_CATEGORY: Int = 30

  final case class ContextualData(
                                   BidRequestId: String,
                                   ContextualCategories: Array[Long]
                                 )
  final case class ContextualDataTier1(
                                        BidRequestId: String,
                                        Tier1Id: Long
                                      )

  final case class ContextualFeature(
                                      BidRequestId: String,
                                      HasContextualCategory: Int,
                                      ContextualCategoryNumber: Int,
                                      ContextualCategoryLength: Double,
                                    )

  final case class ContextualFeatureTier1(
                                           BidRequestId: String,
                                           HasContextualCategoryTier1: Int,
                                           ContextualCategoryNumberTier1: Int,
                                           ContextualCategoryLengthTier1: Double,
                                           ContextualCategoriesTier1: Array[Int],
                                         )

  final case class ContextualFeatureTier1TopN(
                                               BidRequestId: String,
                                               ContextualCategoriesTier1TopN: Array[Long],
                                             )


  final case class ContextualFeatureTier1Weights(
                                                  BidRequestId: String,
                                                  TotalHitsTier1: Long,
                                                  ContextualCategoryWeightsTier1: Array[Double],
                                                )


  def getContextualDataTier1(
                              contextualData: Dataset[ContextualData]
                            )(implicit prometheus: PrometheusClient): Dataset[ContextualDataTier1] = {
    // TODO: efficiency to be optimized
    val ContextualTier1Map = ContextualTaxonomyDataset.readHardCodedTier1Map()

    contextualData
      .select("BidRequestId", "ContextualCategories")
      .withColumn("ContextualCategoryId", explode($"ContextualCategories"))
      .join(ContextualTier1Map, Seq("ContextualCategoryId"), "left")
      .cache()
      .selectAs[ContextualDataTier1]
  }

  def generateContextualFeature(
                                 contextualData: Dataset[ContextualData]
                               )(implicit prometheus: PrometheusClient): Dataset[ContextualFeature] = {
    contextualData
      .withColumn("ContextualCategoryNumber"
      , when(isnull($"ContextualCategories"), 0).otherwise(size($"ContextualCategories")))
      .withColumn("ContextualCategoryLength", $"ContextualCategoryNumber" / lit(CARDINALITY_ALL_CATEGORY.toDouble))
      .withColumn("HasContextualCategory"
        , when(isnull($"ContextualCategories"), 0).otherwise(1))
      .selectAs[ContextualFeature]
  }

  def generateContextualFeatureTier1(
                                      contextualData: DataFrame
                                    ): DataFrame = {
    /* ContextualFeature V2: collect tier1 category list and generate features
       1. ContextualCategoriesTier1
       2. ContextualCategoryNumberTier1
       3. ContextualCategoryLengthTier1
       4. HasContextualCategoryTier1
     */

    val ContextualTier1Map = ContextualTaxonomyDataset.readHardCodedTier1Map()
    val IdMap = spark.sparkContext.broadcast(
      ContextualTier1Map.select("ContextualCategoryId", "Tier1Id").as[(Long, Int)].collect().toMap
    )

    def Tier1IdMapping(allIds: Seq[Long]): Seq[Int] = {
      allIds.map(i => IdMap.value.getOrElse(i, 0)).distinct
    }

    def Tier1IdMappingUdf: UserDefinedFunction = udf(Tier1IdMapping _)

    contextualData.withColumn(
      "ContextualCategoriesTier1",
      when($"ContextualCategories".isNotNull, Tier1IdMappingUdf($"ContextualCategories")).otherwise(null))
      .withColumn(
        "ContextualCategoryNumberTier1",
        when(isnull($"ContextualCategoriesTier1"), 0).otherwise(size($"ContextualCategoriesTier1")))
      .withColumn(
        "HasContextualCategoryTier1",
        when(isnull($"ContextualCategoriesTier1"), 0).otherwise(1))
      .withColumn("ContextualCategoryLengthTier1", $"ContextualCategoryNumberTier1" / lit(CARDINALITY_TIER1_CATEGORY.toDouble))
  }

  def seq2String(list: Seq[Long]): String = {
    val builder = StringBuilder.newBuilder
    list.addString(builder, "|")
    builder.toString()
  }

  def seq2StringUdf = udf(seq2String _)

  def generateContextualFeatureTier1Weights(
                                             contextualData: Dataset[ContextualData]
                                           )(implicit prometheus: PrometheusClient): Dataset[ContextualFeatureTier1Weights] = {
    // TODO: efficiency to be optimized
    /* ContextualFeature V3: generate tier1 category weights
       1. Tier1Id_* := Hits_i/TotalHits
       2. TotalHitsTier1 := sum_i(Hits_i)
     */
    // pivot to count hits for each tier1 id
    var tier1Hits = getContextualDataTier1(contextualData)
      .groupBy("BidRequestId")
      .pivot("Tier1Id").count().drop("null").na.fill(0)
    for (c <- 1 to CARDINALITY_TIER1_CATEGORY) {
      if (tier1Hits.columns.exists(_.equals(s"$c"))) {
        tier1Hits = tier1Hits.withColumnRenamed(
          s"$c",
          s"Tier1Id_$c"
        )
      } else {
        tier1Hits = tier1Hits.withColumn(s"Tier1Id_$c", lit(0))
      }
    }

    // count total hits
    val tier1TotalHits = tier1Hits.columns.collect {
      case x
        if x != "BidRequestId"
      => col(x)
    }.reduce(_ + _)
    tier1Hits = tier1Hits.withColumn("TotalHitsTier1", tier1TotalHits)

    // normalize hits for each tier1 category
    val colnames = (1 to CARDINALITY_TIER1_CATEGORY).map(c => s"Tier1Id_$c")
    colnames.foldLeft(tier1Hits)((df, c) =>
      df.withColumn(s"$c", col(c) / col("TotalHitsTier1"))
    ).na.fill(0)
      .withColumn("ContextualCategoryWeightsTier1", array(colnames.map(col): _*))
      .drop(colnames: _*)
      .selectAs[ContextualFeatureTier1Weights]
  }

  def generateContextualFeatureTier1TopN(
                                      contextualData: Dataset[ContextualData],
                                      topN: Integer
                                    )(implicit prometheus: PrometheusClient): Dataset[ContextualFeatureTier1TopN] = {
    val window = Window.partitionBy($"BidRequestId").orderBy(rand()).orderBy($"count".desc)

    // TODO: efficiency to be optimized
    getContextualDataTier1(contextualData)
      .groupBy("BidRequestId", "Tier1Id")
      .count()
      .withColumn("CountRank", row_number().over(window))
      .filter($"CountRank" <= lit(topN))
      .groupBy("BidRequestId")
      .agg(collect_set("Tier1Id").alias("ContextualCategoriesTier1TopN"))
      .selectAs[ContextualFeatureTier1TopN]
  }
}
