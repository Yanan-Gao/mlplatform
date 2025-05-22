package com.thetradedesk.featurestore.jobs

import com.thetradedesk.featurestore.datasets.ProfileDataset
import com.thetradedesk.spark.TTDSparkContext.spark
import com.thetradedesk.spark.util.prometheus.PrometheusClient
import com.thetradedesk.featurestore.{FeautureAppName, OutputRowCountGaugeName, RunTimeGaugeName, aggLevel, date, partCount}
import com.thetradedesk.featurestore.features.Features._
import com.thetradedesk.featurestore.transform.AggregateTransform._
import com.thetradedesk.featurestore.transform.Merger.joinDataFrames
import org.apache.spark.sql.{Column, Dataset}
import org.apache.spark.sql.functions._

import java.time.LocalDate
import java.time.format.DateTimeFormatter

/** Base class for Spark jobs in feature-store repo
 *
 */
abstract class FeatureStoreBaseJob {

  def jobName: String = ""
  def getPrometheus: PrometheusClient = {
    new PrometheusClient(FeautureAppName, jobName)
  }

  def runTransform(args: Array[String]): Array[(String, Long)] = Array(("", 0))

  def main(args: Array[String]): Unit = {

    val prometheus = getPrometheus
    val jobDurationGauge = prometheus.createGauge(RunTimeGaugeName, "Job execution time in seconds")
    val jobDurationGaugeTimer = jobDurationGauge.startTimer()
    val outputRowsWrittenGauge = prometheus.createGauge(OutputRowCountGaugeName, "Number of rows written", "DataSet")

    val rows = runTransform(args)

    // push metrics of output dataset row counts
    rows.foreach { case (datasetName, rowCount) =>
      outputRowsWrittenGauge.labels(datasetName).set(rowCount)
    }
    // push job runtime
    val runTime = jobDurationGaugeTimer.setDuration()
    prometheus.pushMetrics()

    spark.catalog.clearCache()
    spark.stop()
  }
}


/** Base class for Spark jobs that generate aggregated features
 *
 */
abstract class FeatureStoreAggJob extends FeatureStoreBaseJob {
  def jobConfig: FeatureStoreAggJobConfig = new FeatureStoreAggJobConfig("")

  def catFeatSpecs: Array[CategoryFeatAggSpecs] = jobConfig.catFeatSpecs

  def conFeatSpecs: Array[ContinuousFeatAggSpecs] = jobConfig.conFeatSpecs

  def ratioFeatSpecs: Array[RatioFeatAggSpecs] = jobConfig.ratioFeatSpecs

  def loadInputData(date: LocalDate, lookBack: Int): Dataset[_]

  val salt = "42"

  def getDateStr(date: LocalDate): String = {
    val dtf = DateTimeFormatter.ofPattern("yyyyMMdd")
    date.format(dtf)
  }

  def aggBySpecs(inputDf: Dataset[_],
                 aggLevel: String,
                 window: Int,
                 catFeatSpecs: Array[CategoryFeatAggSpecs],
                 conFeatSpecs: Array[ContinuousFeatAggSpecs],
                 ratioFeatSpecs: Array[RatioFeatAggSpecs]): Dataset[_] = {

    var catDf: Dataset[_] = null

    val aggCols = genAggCols(window, catFeatSpecs) ++ genAggCols(window, conFeatSpecs) ++ genAggCols(window, ratioFeatSpecs)
    catDf = inputDf.groupBy(aggLevel).agg(aggCols.head, aggCols.tail: _*)
        .withColumnRenamed(aggLevel, "FeatureKey")

    // hash aggregated categorical features
    val aggCatCols = catFeatSpecs.filter(_.aggWindowDay == window)
    val hashCols = if (!aggCatCols.isEmpty) {
      aggCatCols.map(
        c => if (c.dataType.startsWith("array")) {
          hashFeature(c.featureName, c.dataType, c.cardinality)
        } else hashFeature(c.featureName, s"array_${c.dataType}", c.cardinality)
      )
    } else Array.empty[Column]

    catDf.select(col("*") +: hashCols: _*)

  }

  override def runTransform(args: Array[String]): Array[(String, Long)] = {
    // loop through each lookback window
    val outputDfs: Seq[Dataset[_]] = (catFeatSpecs ++ conFeatSpecs ++ ratioFeatSpecs).groupBy(_.aggWindowDay).keys.par.map { window => {
      // load data
      val inputDf = loadInputData(date, window - 1)
      // aggregations
      aggBySpecs(inputDf, aggLevel, window, catFeatSpecs, conFeatSpecs, ratioFeatSpecs)
    }
    }.toList

    // merge data from different readRange
    val resDf = outputDfs.reduceLeft((df1: Dataset[_], df2: Dataset[_]) => joinDataFrames(df1, df2))
    val rows = ProfileDataset(sourcePartition = jobName, indexPartition = aggLevel).writeWithRowCountLog(resDf, date, Some(partCount.AggResult))

    Array(rows)

  }

}
