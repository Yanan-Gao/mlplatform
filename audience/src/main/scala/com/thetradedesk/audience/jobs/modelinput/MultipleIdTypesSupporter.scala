package com.thetradedesk.audience.jobs.modelinput

import com.thetradedesk.audience.configs.AudienceModelInputGeneratorConfig
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._
import com.thetradedesk.spark.TTDSparkContext.spark.implicits._

import scala.collection.mutable.HashSet

object MultipleIdTypesSupporter {
  def mergeLabels(multipleLabels: DataFrame, columns: Seq[String] = Seq())  = {
    multipleLabels
      .withColumn("ZippedTargets", arrays_zip('SyntheticIds, 'Targets))
      .repartition(16384, 'BidRequestId)
      .groupBy('BidRequestId)
      .agg(
        flatten(collect_list('ZippedTargets)).as("ZippedTargets"),
        (columns ++: Seq("TDID", "idType")).map(e => collect_list(col(e)).as(e + "s")):_*)
      // Find the smallest value in the array
      .withColumn("idType", array_min('idTypes))
      // Find the 1-based index
      .withColumn("idTypeIndex", array_position('idTypes, 'idType).cast("int"))
      // merge results and partialy explode the result to keep the target array within the max length
      .withColumn("ZippedTargets", explode(mergePairs(AudienceModelInputGeneratorConfig.labelMaxLength)('ZippedTargets)))
      .select((columns ++: Seq("TDID")).map(e => element_at(col(e + "s"), 'idTypeIndex).as(e)) ++:
        Seq(
          col("BidRequestId"),
          col("idType"),
          col("ZippedTargets").getField("_1").as("SyntheticIds"),
          col("ZippedTargets").getField("_2").as("Targets")
        ): _*)
  }

  def mergePairs(maxLength: Int) = udf(
    (Pairs: Array[(Int, Boolean)]) => {
      val posSyntheticIds = HashSet[Int]()
      val negSyntheticIds = HashSet[Int]()

      Pairs.foreach(
        e => if (e._2) posSyntheticIds.add(e._1) else negSyntheticIds.add(e._1)
      )

      negSyntheticIds --= posSyntheticIds

      (posSyntheticIds.toArray.map(id => (id, 1f)) ++
        negSyntheticIds.toArray.map(id => (id, 0f))).grouped(maxLength).toArray
    }
  )
}
