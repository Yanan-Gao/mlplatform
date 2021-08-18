package com.ttd.ml.features.web.classification.metrics

import java.time.LocalDate
import com.ttd.ml.datasets.generated.contextual.web.classification.{ContentClassificationStickinessRecord, IABContentClassification, IABContentClassificationScale, IABContentClassificationStickiness}
import com.ttd.ml.datasets.sources.IABCategories
import com.ttd.ml.util.elDoradoUtilities.spark.TTDConfig.config
import com.ttd.ml.spark.TTDSparkContext.spark.implicits._
import org.apache.spark.sql.functions.{arrays_zip, slice, udf}

class StickinessConfig {
  val ApplicationName = "StickinessWorkflow"
  val Environment: String = config.getString("ttd.env", "local")
  val date: LocalDate = config.getDate("date", LocalDate.now().minusDays(2))

  // Stickiness parameters
  val topk: Int = config.getInt("topk", 5)
  val dayTimeDifference: Int = config.getInt("dayTimeDifference", 5)
  val sampleSize: Int = config.getInt("sampleSize", 1000)
  val levels: Seq[Int] = config.getStringSeq("levels", Seq("1","2")).map(_.toInt)

  // Parallelism parameters
  val fileCount: Int = config.getInt("fileCount", 1)
  val tempDir: String = config.getString("tempDir", "hdfs:///user/hadoop/output-temp-dir")
}

object StickinessWorkflow {
  val explodeClasses: Seq[String] => Seq[String] =
    // gets all the parents of this class
    (classNames: Seq[String]) => {
      classNames.flatMap(name =>
        name.split(", ").inits.toList.map(_.mkString(", ")).filter(_.length > 0)
      )
    }

  def run[T<:StickinessConfig](config: T): Unit = {
    val explodeClassesUDF = udf(explodeClasses)
    val date = config.date

    val day1 = IABContentClassification()
      .readDate(date)
      .select('Url, slice(arrays_zip($"preds.class", $"preds.score"), 1, config.topk) as "predictions")
      .select('Url, explodeClassesUDF($"predictions.0") as "Category1")
      .cache

    val day2 = IABContentClassification()
      .readDate(date.minusDays(config.dayTimeDifference))
      .select('Url, slice(arrays_zip($"preds.class", $"preds.score"), 1, config.topk) as "predictions")
      .select('Url, explodeClassesUDF($"predictions.0") as "Category2")
      .cache

    val parentMap = IABCategories().readLatestPartition()
      .select('Id, 'Parent)
    val pMap: Map[Int, Int] = parentMap.as[(Int, Int)].collect().toMap

    val stickinessTransformer = new CategoryStickiness(
      parentMap = pMap,
      sampleSize = config.sampleSize,
      levels = config.levels,
      predictionCol1 = "Category1",
      predictionCol2 = "Category2"
    )

    val ds = stickinessTransformer
      .transform(day1, day2)
      .as[ContentClassificationStickinessRecord]

    IABContentClassificationStickiness().writePartition(ds, date, config.fileCount, config.tempDir)
  }

  def main(args: Array[String]): Unit = {
    run(new StickinessConfig)
  }
}
