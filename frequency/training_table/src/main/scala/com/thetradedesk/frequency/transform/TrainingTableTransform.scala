package com.thetradedesk.frequency.transform

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._

object TrainingTableTransform {

  def buildOffsetLookup(aggregateSets: Seq[Seq[Int]]): Map[Int, Seq[Int]] = {
    aggregateSets.foldLeft(Map.empty[Int, Seq[Int]]) { (acc, offsets) =>
      if (offsets.isEmpty) acc else acc + (offsets.max -> offsets)
    }
  }

  def computeLongWindowApprox(
      df: DataFrame,
      dayLookup: Map[Int, Seq[Int]],
      ratioCol: String,
      sameDayImpsCol: String,
      sameDayClicksCol: String,
      aggImpressionPrefix: String,
      aggClickPrefix: String,
      dayImpressionPrefix: String,
      dayClickPrefix: String,
      outImpsPrefix: String,
      outClickPrefix: String
  ): DataFrame = {
    dayLookup.toSeq.sortBy(_._1).foldLeft(df) { case (acc, (lastDay, offsets)) =>
      val offsetsSorted = offsets.sorted
      val baseName = offsetsSorted.mkString("_")
      val impsOffsetsCol = if (offsetsSorted.length > 1) s"$aggImpressionPrefix$baseName" else s"$dayImpressionPrefix${offsetsSorted.head}"
      val clickOffsetsCol = if (offsetsSorted.length > 1) s"$aggClickPrefix$baseName" else s"$dayClickPrefix${offsetsSorted.head}"

      val lastImpsCol = s"$dayImpressionPrefix$lastDay"
      val lastClicksCol = s"$dayClickPrefix$lastDay"

      val required = Seq(impsOffsetsCol, clickOffsetsCol, lastImpsCol, lastClicksCol)
      required.foreach { c =>
        if (!acc.columns.contains(c)) throw new IllegalArgumentException(s"Missing required column '$c' for last_day=$lastDay")
      }

      val outImpsCol = s"$outImpsPrefix${lastDay}d_approx"
      val outClicksCol = s"$outClickPrefix${lastDay}d_approx"

      acc.withColumn(outImpsCol, col(sameDayImpsCol) + col(impsOffsetsCol) + col(lastImpsCol) * (lit(1.0) - col(ratioCol)))
        .withColumn(outClicksCol, col(sameDayClicksCol) + col(clickOffsetsCol) + col(lastClicksCol) * (lit(1.0) - col(ratioCol)))
    }
  }
}
