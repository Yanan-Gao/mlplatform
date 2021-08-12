package com.ttd.ml.features.web.classification.metrics

import java.time.LocalDate

import com.swoop.alchemy.spark.expressions.hll.functions.{hll_init_agg, hll_merge, hll_row_merge}
import com.ttd.ml.datasets.generated.contextual.web.classification.{ContentClassificationScaleRecord, IABContentClassification, IABContentClassificationScale}
import com.ttd.ml.datasets.sources.IABCategories
import com.ttd.ml.util.elDoradoUtilities.spark.TTDConfig.config
import com.ttd.ml.spark.TTDSparkContext.spark.implicits._
import org.apache.spark.sql.functions._

class ScaleConfig {
  val ApplicationName = "ScaleWorkflow"
  val Environment: String = config.getString("ttd.env", "local")
  val date: LocalDate = config.getDate("date", LocalDate.now().minusDays(1))

  // Scale parameters
  val topk: Int = config.getInt("topk", 3)
  val relativeSD: Double = config.getDouble("relativeSD", 0.04)
  val numBuckets: Int = config.getInt("numBuckets", 25)

  // Parallelism parameters
  val fileCount: Int = config.getInt("fileCount", 1)
  val tempDir: String = config.getString("tempDir", "hdfs:///user/hadoop/output-temp-dir")
}

object ScaleWorkflow {
  def run[T<:ScaleConfig](config: T): Unit = {
    val date = config.date
    val topk = config.topk
    val numBuckets = config.numBuckets
    val interval = 100.0 / numBuckets.toDouble
    val buckets = (1 to numBuckets map (b => math.floor(b * interval).toInt)).reverse

    val classifications = IABContentClassification()
      .readDate(date)
      .select('Url, explode(slice(arrays_zip('Categories, 'Confidences), 1, topk)) as "predictions")
      .select('Url, $"predictions.0" as "Category", $"predictions.1" as "Confidence")

    val TDIDhllSketches = classifications
      // Use negative bucket so that we get reversed cumulative distribution
      // This way we can directly read off how much scale we get at each threshold
      // (since we would keep all urls above the threshold)
      .withColumn("ConfidenceBucket", -floor(floor(('Confidence * 100) / interval) * interval)) // express confidence as a percentage (rounded to nearest 4%)
      .groupBy('Category, 'ConfidenceBucket)
      .agg(hll_init_agg('Url, relativeSD = config.relativeSD) as "hll_id")

    // merge parent-child categories to get full scale of parents
    val parentMap = IABCategories().readLatestPartition()
      .select('Id, 'Parent)

    val pMap: Map[Int, Int] = parentMap.as[(Int, Int)].collect().toMap
    val childrenMerger = new MergeChildrenSketches(parentMap = pMap, buckets = buckets)
      .setIDCol("UniqueID")
      .setBucketCol("ConfidenceBucket")
      .setSketchCol("hll_id")

    val mergedSketches = childrenMerger.transform(TDIDhllSketches)

    val hllTransformer = new HLLCumulativeAggregator(numBuckets = numBuckets)
      .setAggCol("UniqueID")
      .setBucketCol("ConfidenceBucket")
      .setSketchCol("hll_id")

    val coherence = hllTransformer.transform(mergedSketches)
    val ds = coherence
      .select('UniqueID, lit(buckets.toArray) as "ConfidenceBuckets", 'cumulativeDistribution as "CumulativeDistribution")
      .as[ContentClassificationScaleRecord]

    IABContentClassificationScale().writePartition(ds, date, config.fileCount, config.tempDir)
  }

  def main(args: Array[String]): Unit = {
    run(new ScaleConfig)
  }
}
