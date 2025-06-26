package com.thetradedesk.featurestore.jobs

import com.thetradedesk.featurestore._
import com.thetradedesk.featurestore.datasets.{ProcessedDataset, ProfileDataset}
import com.thetradedesk.featurestore.features.Features._
import com.thetradedesk.featurestore.transform.AggregateTransform._
import com.thetradedesk.featurestore.transform.Merger.joinDataFrames
import com.thetradedesk.spark.TTDSparkContext
import com.thetradedesk.spark.TTDSparkContext.spark
import com.thetradedesk.spark.util.prometheus.PrometheusClient
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{Column, Dataset, SaveMode}

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

    if (ttdEnv == "local") {
      TTDSparkContext.setTestMode()
    }

    if (ttdEnv == "local") {
      runTransform(args)
    } else {
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
    }

    spark.catalog.clearCache()
    spark.stop()
  }

  def getDateStr(date: LocalDate): String = {
    val dtf = DateTimeFormatter.ofPattern("yyyyMMdd")
    date.format(dtf)
  }
}


/** Base class for Spark jobs that generate aggregated features
 *
 */
abstract class FeatureStoreAggJob extends FeatureStoreBaseJob {

  override def jobName: String = s"${getClass.getSimpleName.stripSuffix("$")}"

  val profileDataPath = "features/feature_store/{ttdEnv}/profiles/source={sourcePartition}/index={indexPartition}/job=${jobName}/v=1/"

  val sourcePartition: String

  def jobConfig = new FeatureStoreAggJobConfig(s"${getClass.getSimpleName.stripSuffix("$")}.json")

  def catFeatSpecs: Array[CategoryFeatAggSpecs] = jobConfig.catFeatSpecs

  def conFeatSpecs: Array[ContinuousFeatAggSpecs] = jobConfig.conFeatSpecs

  def ratioFeatSpecs: Array[RatioFeatAggSpecs] = jobConfig.ratioFeatSpecs

  def loadInputData(date: LocalDate, lookBack: Int): Dataset[_]

  val salt = "42"

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
    val overridesMap = Map(
      "sourcePartition" -> sourcePartition,
      "jobName" -> jobName,
      "indexPartition" -> aggLevel,
      "dateStr" -> getDateStr(date),
      "ttdEnv" -> ttdEnv
    )

    val profileDataset = ProfileDataset(prefix = profileDataPath, overrides = overridesMap)
    if (!overrideOutput && profileDataset.isProcessed) {
      println(s"ProfileDataSet ${profileDataset.datasetPath} existed, skip processing " +
        s"source ${sourcePartition}, aggLevel ${aggLevel}, aggregation job ${jobName}")
      return Array(("", 0))
    }

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

    val rows = profileDataset.writeWithRowCountLog(resDf)

    Array(rows)

  }

}

abstract class FeatureStoreGenJob[T <: Product] extends FeatureStoreBaseJob {

  override def jobName: String = s"${getClass.getSimpleName.stripSuffix("$")}"

  override def runTransform(args: Array[String]): Array[(String, Long)] = {
    val dataset = initDataSet()
    val dataSetName = dataset.getClass.getSimpleName

    if (!overrideOutput && dataset.isProcessed(date)) {
      println(s"DataSet ${dataSetName} existed and override disabled")
      return Array((dataSetName, 0))
    }

    val generatedData = generateDataSet()
    dataset.writePartition(
      generatedData,
      date,
      saveMode = SaveMode.Overwrite
    )
    Array((dataSetName, generatedData.count()))
  }

  def generateDataSet(): Dataset[T]

  def initDataSet(): ProcessedDataset[T]
}
