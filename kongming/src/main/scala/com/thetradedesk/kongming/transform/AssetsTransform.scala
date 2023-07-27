package com.thetradedesk.kongming.transform


import com.thetradedesk.kongming.datasets.{BidsImpressionsSchema, ImpressionPlacementIdSchema}
import com.thetradedesk.spark.sql.SQLFunctions._
import com.thetradedesk.spark.TTDSparkContext.spark.implicits._
import com.thetradedesk.spark.util.prometheus.PrometheusClient

import org.apache.spark.sql.Dataset
import org.apache.spark.sql.functions._
import org.apache.spark.sql.expressions.Window


object AssetsTransform {

  final case class SiteImpressionPlacementRecord(
                                                  Site: Option[String],
                                                  ImpressionPlacementId: Option[String],
                                                )

  /**
   * Calculate top frequent placement for each site occurred in bidsimpression. More placement id will be preserved
   * for top sites. A site is top or not is decided on count(bidrequest) ranking.
   *
   * @param bidsImpressions  bidsimpression dataframe
   * @param topSiteCut  The cutoff to define top sites and others based on count(bidrequest) rank.
   * @param placementCutTop  The number of placement id preserved for top sites
   * @param placementCutTail  The number of placement id preserved for others
   * @return The transformed DataFrame.
   */
  def topPlacementPerSite(
                         bidsImpressions: Dataset[BidsImpressionsSchema],
                         topSiteCut: Int = 3000,
                         placementCutTop: Int = 50,
                         placementCutTail: Int = 20,
                         )(implicit prometheus: PrometheusClient): Dataset[ImpressionPlacementIdSchema] = {

    val sitePlacementDataset = bidsImpressions.select("Site", "ImpressionPlacementId").selectAs[SiteImpressionPlacementRecord]

    // calculate placement distribution and ranking per site
    val windowSite = Window.partitionBy("Site")
    val sitePlmAgg = sitePlacementDataset.groupBy("Site", "ImpressionPlacementId").count()
      .withColumn("countSite", sum($"count").over(windowSite))
      .withColumn("placementPct", $"count" /$"countSite")
    val windowSiteByPlmPct = Window.partitionBy("Site").orderBy($"placementPct".desc)
    val sitePlacementDF = sitePlmAgg.withColumn("placementRank", rank().over(windowSiteByPlmPct))
      .withColumn("placementCumuPct", sum($"placementPct").over(windowSiteByPlmPct)).cache()

    // calculate site ranking
    val topSiteDF = sitePlacementDF.groupBy("Site").agg(sum("count").alias("countSite"))
      .withColumn("countAll", sum($"countSite").over())
      .withColumn("sitePct", $"countSite" /$"countAll")
      .withColumn("siteRank", rank().over(Window.orderBy($"sitePct".desc)))

    val sitePlacementRankDF = sitePlacementDF.join(broadcast(topSiteDF), Seq("Site"), "inner")

    // preserve more placement id for top sites.
    // To minimize data size, exclude placements that either hit the cutoff or cumulative percentage > 95%
    sitePlacementRankDF.filter(($"siteRank" <= topSiteCut) && ($"placementRank" <= placementCutTop) && ($"placementCumuPct" < 0.95))
      .union(
        sitePlacementRankDF.filter(($"siteRank" > topSiteCut) && ($"placementRank" <= placementCutTail) && ($"placementCumuPct" < 0.95))
      )
      .selectAs[ImpressionPlacementIdSchema].distinct()

  }
}
