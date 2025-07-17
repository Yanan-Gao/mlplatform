package com.thetradedesk.featurestore.jobs

import com.thetradedesk.featurestore._
import com.thetradedesk.featurestore.aggfunctions.AggFuncProcessorFactory
import com.thetradedesk.featurestore.aggfunctions.AggFunctions.AggFuncV2.getExportFuncs
import com.thetradedesk.featurestore.configs.{AggDefinition, AggLevelConfig}
import com.thetradedesk.featurestore.constants.FeatureConstants.{ColFeatureKey, ColFeatureKeyCount}
import com.thetradedesk.featurestore.datasets.ProfileDataset
import com.thetradedesk.featurestore.jobs.AggAttributions.sourcePartition
import com.thetradedesk.featurestore.transform.Merger.joinDataFrames
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._
import org.apache.spark.storage.StorageLevel


object RollupAggJob extends FeatureStoreAggBaseJob {
  override def jobName: String = s"${getClass.getSimpleName.stripSuffix("$")}"

  // Loads configuration, validates output existence, and coordinates processing
  override def runTransform(args: Array[String]): Array[(String, Long)] = {
    val aggDef = AggDefinition.loadConfig(dataSource = Some(aggDataSource))
    val aggLevelConfig = aggDef.aggLevels.find(_.level == aggLevel).getOrElse(
      throw new IllegalArgumentException(s"AggLevel $aggLevel not found in configuration")
    )

    val overridesMap = Map(
      "sourcePartition" -> sourcePartition,
      "jobName" -> jobName,
      "indexPartition" -> aggLevel,
      "dateStr" -> getDateStr(date),
      "ttdEnv" -> ttdEnv,
      "grain" -> aggLevelConfig.initAggGrains.max.toString
    )

    val outputDataSet = ProfileDataset(
      rootPath = aggDef.rollupAggConfig.outputRootPath,
      prefix = aggDef.rollupAggConfig.outputPrefix,
      overrides = overridesMap
    )

    if (!overrideOutput && outputDataSet.isProcessed) {
      println(s"ProfileDataSet ${outputDataSet.datasetPath} already exists, skipping processing for " +
        s"source: $sourcePartition, aggLevel: ${aggLevelConfig.level}, job: $jobName")
      return Array(("RollupAggJob_" + aggDataSource, 0))
    }

    // get a set of all aggregation windows and sort them in ascending order
    val windowsToAggregate = aggDef.aggregations.flatMap(_.aggWindows).distinct.sorted
    println(s"Processing Rollup Agg for DataSource ${aggDataSource}, aggLevel ${aggLevelConfig.level}, aggregation windows: ${windowsToAggregate.mkString(", ")}")

    // Use incremental aggregation to avoid redundant data loading and computation
    val resDf = processIncrementalAggregation(aggDef, overridesMap, windowsToAggregate, aggLevelConfig)

    outputDataSet.writeWithRowCountLog(resDf, aggLevelConfig.rollupWritePartitions)

    Array(("RollupAggJob_" + aggDataSource, 0))
  }

  /**
   * Process aggregation windows incrementally to avoid redundant data loading and computation
   *
   * @param aggDef       Aggregation definition
   * @param overridesMap Overrides map
   * @param aggWindows   Sorted list of aggregation windows
   * @return Final aggregated DataFrame
   */
  private def processIncrementalAggregation(
                                             aggDef: AggDefinition,
                                             overridesMap: Map[String, String],
                                             aggWindows: Seq[Int],
                                             aggLevelConfig: AggLevelConfig
                                           ): DataFrame = {

    val rawAggResultByWindow = scala.collection.mutable.Map[Int, DataFrame]()

    aggWindows.foldLeft(Option.empty[DataFrame]) { (acc, window) =>
      println(s"Processing Rollup Agg for DataSource ${aggDataSource}, aggLevel ${aggLevel}, window $window...")

      val windowResult = if (window == 1) {
        val inputDf = loadInitialAggDataWindow(aggDef.initAggConfig, overridesMap)
        // we should just return the inputDf if the windowGrain is the same as the initAggGrain, but seeing failure of TaskResultLost (result lost from block manager)
        //        val aggResult = if (aggDef.windowGrain == aggLevelConfig.initAggGrains.max) inputDf else aggByDefinition(inputDf, aggDef)
        val aggResult = aggByDefinition(inputDf, aggDef, aggLevelConfig)
        rawAggResultByWindow(window) = aggResult
        selectByDefinition(aggResult, aggDef, window, aggLevelConfig)
      } else {
        computeIncrementalAggregation(aggDef, overridesMap, window, rawAggResultByWindow, aggLevelConfig)
      }

      val persistedResult = windowResult.persist(StorageLevel.MEMORY_AND_DISK)

      acc match {
        case Some(previousDf) => Some(joinDataFrames(previousDf, persistedResult))
        case None => Some(persistedResult)
      }
    }.getOrElse(throw new RuntimeException("No windows to process"))
  }

  /**
   * Compute aggregation for a window using incremental approach
   * Reuses existing window results to compute larger windows
   * Falls back to full computation if no smaller window results exist
   *
   * @param aggDef               Aggregation definition
   * @param overridesMap         Overrides map
   * @param window               Current window size
   * @param rawAggResultByWindow Map of existing window results
   * @return Aggregated DataFrame for the current window
   */
  private def computeIncrementalAggregation(
                                             aggDef: AggDefinition,
                                             overridesMap: Map[String, String],
                                             window: Int,
                                             rawAggResultByWindow: scala.collection.mutable.Map[Int, DataFrame],
                                             aLevel: AggLevelConfig
                                           ): DataFrame = {

    val maxAggregatedWindow = rawAggResultByWindow.keys.toSeq.sorted.lastOption.filter(_ < window)

    maxAggregatedWindow match {

      // take previous agg rest then union with balance
      case Some(maxWindow) =>
        val maxAggregatedResult = rawAggResultByWindow(maxWindow)
        val restDaysDf = loadInitialAggDataWindow(aggDef.initAggConfig, overridesMap, maxWindow, window - 1)

        if (restDaysDf.isEmpty) {
          println(s"Warning: No data found for window [$maxWindow, ${window - 1}], directly return result of window $maxWindow")
          rawAggResultByWindow(window) = maxAggregatedResult
          selectByDefinition(maxAggregatedResult, aggDef, window, aLevel)
        } else {
          val rawAggResult = aggByDefinition(maxAggregatedResult.unionByName(restDaysDf, allowMissingColumns = true), aggDef, aLevel)
          rawAggResultByWindow(window) = rawAggResult
          selectByDefinition(rawAggResult, aggDef, window, aLevel)
        }

      // load all initial result and do aggregation
      case None =>
        val rawAggResult = aggByDefinition(loadInitialAggDataWindow(aggDef.initAggConfig, overridesMap, 0, window - 1), aggDef, aLevel)
        rawAggResultByWindow(window) = rawAggResult
        selectByDefinition(rawAggResult, aggDef, window, aLevel)
    }
  }

  def aggAndSelectByDefinition(
                                inputDf: DataFrame,
                                aggDef: AggDefinition,
                                window: Int,
                                aLevel: AggLevelConfig
                              ): DataFrame = {
    val aggResult = aggByDefinition(inputDf, aggDef, aLevel)
    selectByDefinition(aggResult, aggDef, window, aLevel)
  }

  // Performs the actual aggregation by grouping by feature key and applying the aggregation functions
  def aggByDefinition(
                       inputDf: DataFrame,
                       aggDef: AggDefinition,
                       aggLevelConfig: AggLevelConfig
                     ): DataFrame = {
    val initAggDef = aggDef.extractInitialAggDefinition()
    val mergeCols = if (aggLevelConfig.enableFeatureKeyCount) genKeyCountMergeCol() ++ genMergeCols(initAggDef) else genMergeCols(initAggDef)

    inputDf
      .groupBy(col(ColFeatureKey))
      .agg(mergeCols.head, mergeCols.tail: _*)
  }

  // Creates time-windowed feature columns from raw aggregations
  def selectByDefinition(
                          aggResult: DataFrame,
                          aggDef: AggDefinition,
                          window: Int,
                          aggLevelConfig: AggLevelConfig
                        ): DataFrame = {
    val metaColumns = if (aggLevelConfig.enableFeatureKeyCount) {
      Seq(col(ColFeatureKey), col(ColFeatureKeyCount).alias(s"${ColFeatureKeyCount}_Last${window}D"))
    } else {
      Seq(col(ColFeatureKey))
    }

    // Filter aggregations for the current window
    val windowAggregations = aggDef.aggregations.filter(_.aggWindows.contains(window))
    if (windowAggregations.isEmpty) {
      println(s"Warning: No aggregations found for window $window")
      return aggResult
    }

    // Generate all aggregation columns in a single pass
    val aggColumns = windowAggregations.flatMap { fieldAggSpec =>
      val exportFunctions = getExportFuncs(fieldAggSpec.aggFuncs)
      exportFunctions.flatMap { func =>
        val processor = AggFuncProcessorFactory.getProcessor(func, fieldAggSpec)
        processor.export(window, aggDef.rollupAggConfig.windowGrain)
      }
    }

    // Add all new columns in a single select operation
    aggResult.select(metaColumns ++ aggColumns: _*)
  }
}

