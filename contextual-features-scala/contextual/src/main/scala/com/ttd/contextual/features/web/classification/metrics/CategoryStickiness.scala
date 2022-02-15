package com.ttd.contextual.features.web.classification.metrics

import com.ttd.contextual.spark.TTDSparkContext.spark.implicits._
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._

/**
 * Calculates stickiness for each category over two dataframes representing different periods of time
 */
class CategoryStickiness(
                        val levels: Seq[Int] = Seq(1,2),
                        val sampleSize: Int = 1000,
                        val urlCol: String = "Url",
                        val predictionCol1: String = "Categories1",
                        val predictionCol2: String = "Categories2",
                        val parentMap: Map[Int, Int]
                        ) extends Serializable {
  val prefixLevel: Int => ((String) => String) =
    (n: Int) => {
      (url: String) => {
        url.split('?').head.split('/').take(n).mkString("/")
      }
    }

  /***
   * Similar to jaccard similirity score of classifications for Urls which share the same (domain + prefix)
   * This could be adapted to get the KL divergence instead that accounts for probabilities.
   */
  def stickinessScore: (Seq[Int], Seq[Int]) => Seq[(Int, Boolean)] =
  // category is true if both have it, false if not
    (a: Seq[Int], b: Seq[Int]) => {
      val intersection = a.intersect(b)
      val union = a.union(b).distinct
      union.map(elem => (elem, intersection.contains(elem)))
    }

  val explodeClasses: Seq[Int] => Seq[Int] =
  // gets all the parents of this class
    (categoryIds: Seq[Int]) => {
      categoryIds.flatMap(id =>
        allParentsMap.getOrElse(id, Seq())
      )
    }

  def getAllParents: Int => Seq[Int] = id => {
    if (!parentMap.contains(id)) {
      Seq()
    } else {
      id +: getAllParents(parentMap.apply(id))
    }
  }

  val allParentsMap: Map[Int, Seq[Int]] = parentMap.keys.map(id => (id, getAllParents(id))).toMap
  val joinDF: (DataFrame, DataFrame) => DataFrame = (a,b) => a.join(b, "Category")

  def transform(ds1: DataFrame, ds2: DataFrame): DataFrame = {
    val stickinessUDF = udf(stickinessScore(_,_))
    val explodeClassesUDF = udf(explodeClasses(_))
    levels.map {
      l =>
        val window = Window.partitionBy("Url").orderBy(rand)
        val rankByScore = row_number().over(window)
        val levelUDF = udf(prefixLevel(l)(_))

        val d1 = ds1.withColumn(urlCol, levelUDF(col(urlCol)))
          .select('*, rankByScore as "rank")
          .filter('rank < sampleSize)
        val d2 = ds2.withColumn(urlCol, levelUDF(col(urlCol)))
          .select('*, rankByScore as "rank")
          .filter('rank < sampleSize)

        d1
          .join(d2, urlCol)
          .select(explode(
            stickinessUDF(explodeClassesUDF(col(predictionCol1)),
                          explodeClassesUDF(col(predictionCol2)))) as "catScore")
          .select($"catScore._1" as "Category", $"catScore._2" as "Stickiness")
          .groupBy($"Category")
          .agg(avg('Stickiness cast("int")) as s"Stickiness$l") // std dev and n
    }.reduce(joinDF)
  }
}


