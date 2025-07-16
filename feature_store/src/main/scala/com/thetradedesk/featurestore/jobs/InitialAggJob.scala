package com.thetradedesk.featurestore.jobs

import com.thetradedesk.featurestore._
import com.thetradedesk.featurestore.aggfunctions.{AggFuncProcessorFactory, AggFuncProcessorUtils}
import com.thetradedesk.featurestore.configs.{AggDefinition, AggLevelConfig}
import com.thetradedesk.featurestore.constants.FeatureConstants.ColFeatureKey
import com.thetradedesk.featurestore.datasets.ProfileDataset
import com.thetradedesk.featurestore.jobs.AggAttributions.sourcePartition
import com.thetradedesk.featurestore.rsm.CommonEnums
import com.thetradedesk.featurestore.rsm.CommonEnums.Grain.Hourly
import com.thetradedesk.featurestore.transform.DescriptionAgg.genKeyCountCol
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{Column, DataFrame, Dataset}

// This job can be run hourly/daily, do the base agg from datasource configuration
object InitialAggJob extends FeatureStoreAggBaseJob {
  override def jobName: String = s"${getClass.getSimpleName.stripSuffix("$")}"

  val grainEnum = CommonEnums.Grain
    .fromString(grain)
    .getOrElse(
      throw new IllegalArgumentException(
        s"Grain is not specified in the job config"
      )
    )

  override def runTransform(args: Array[String]): Array[(String, Long)] = {

    val aggDef = AggDefinition.loadConfig(dataSource = Some(aggDataSource))
    val initialAggDef = aggDef.extractInitialAggDefinition()
    val baseOverrides = Map(
      "sourcePartition" -> sourcePartition,
      "jobName" -> jobName,
      "dateStr" -> getDateStr(date),
      "ttdEnv" -> ttdEnv,
      "grain" -> grainEnum.toString,
      "year" -> date.getYear().toString,
      "monthMM" -> f"${date.getMonthValue()}%02d",
      "dayDD" -> f"${date.getDayOfMonth()}%02d",
    )

    // load raw and process all levels in a batch for each hour
    if (grainEnum == Hourly) {
      hourArray.foreach { hour =>
        val overrides = baseOverrides + ("hourInt" -> hour.toString)

        // we want to batch process all aggLevels since raw data can be loaded only once
        loadAndProcess(
          initialAggDef,
          overrides,
          initialAggDef.aggLevels
            .filter(x => x.initAggGrains.contains(Hourly))
            .toSeq
        )
      }
    } else {
      // do merge if exists hourly level
      initialAggDef.aggLevels
        .filter(x => x.initAggGrains.contains(grainEnum) && x.initAggGrains.exists(level => level < grainEnum))
        .foreach(x => {
          mergeInitialAggResults(
            initialAggDef,
            baseOverrides + ("indexPartition" -> x.level),
            x
          )
        })

      // load raw and process
      loadAndProcess(
        initialAggDef,
        baseOverrides,
        initialAggDef.aggLevels
          .filter(x => !x.initAggGrains.exists(level => level < grainEnum))
          .toSeq
      )
    }

    Array(("BaseAgg" + aggDataSource, 0))
  }

  private def mergeInitialAggResults(
                                      aggDefinition: AggDefinition,
                                      overridesMap: Map[String, String],
                                      aggLevel: AggLevelConfig
                                    ): Unit = {
    val outputDataSet = ProfileDataset(
      rootPath = aggDefinition.initAggConfig.outputRootPath,
      prefix = aggDefinition.initAggConfig.outputPrefix,
      grain = Some(
        grainEnum
      ), // use current grain to check if the level is already processed
      overrides = overridesMap
    )
    if (!overrideOutput && outputDataSet.isProcessed) {
      println(
        s"Initial Agg Level ${aggLevel.level} is already processed: ${outputDataSet.datasetPath}"
      )
      return
    }
    // load previous hourly level agg results and merge
    println(s"Merging Initial Agg at Level ${aggLevel.level}, Grain: ${grainEnum}, outputPath: ${outputDataSet.datasetPath}")
    val aggInputDf = loadInitialAggDataWindow(aggDefinition.initAggConfig, overridesMap + ("grain" -> Hourly.toString)) // override grain as hourly to load previous hourly level agg results

    val result = mergeByDefinition(aggInputDf, aggDefinition, aggLevel)
    outputDataSet.writeWithRowCountLog(result, Some(aggLevel.initWritePartitions.getOrElse(1) * 10))
  }

  def mergeByDefinition(
                         inputDf: DataFrame,
                         aggDef: AggDefinition,
                         aggLevel: AggLevelConfig
                       ): DataFrame = {

    val mergeCols =
      if (aggLevel.enableFeatureKeyCount)
        genKeyCountMergeCol() ++ genMergeCols(aggDef)
      else genMergeCols(aggDef)

    inputDf
      .groupBy(col(ColFeatureKey))
      .agg(mergeCols.head, mergeCols.tail: _*)
  }

  // load raw data source once and process all levels in a batch
  private def loadAndProcess(
                              initialAggDef: AggDefinition,
                              overridesMap: Map[String, String],
                              aggLevelsToProcess: Seq[AggLevelConfig]
                            ): Unit = {
    if (aggLevelsToProcess.isEmpty) {
      return
    }
    val filteredLevels = aggLevelsToProcess.filter(x => {

      val overrides = overridesMap + ("indexPartition" -> x.level)
      // validate output
      val outputDataSet = ProfileDataset(
        rootPath = initialAggDef.initAggConfig.outputRootPath,
        prefix = initialAggDef.initAggConfig.outputPrefix,
        grain = Some(grainEnum),
        overrides = overrides
      )

      overrideOutput || !outputDataSet.isProcessed
    })

    if (filteredLevels.isEmpty) {
      println(
        s"All Agg levels for source ${aggDataSource} are already processed, overridesMap: $overridesMap"
      )
      return
    }
    // load raw data source once
    val inputDf = loadDataSource(initialAggDef, overridesMap, filteredLevels)

    filteredLevels.foreach { aLevel =>
      val outputDataSet = ProfileDataset(
        rootPath = initialAggDef.initAggConfig.outputRootPath,
        prefix = initialAggDef.initAggConfig.outputPrefix,
        grain = Some(grainEnum),
        overrides = overridesMap + ("indexPartition" -> aLevel.level)
      )
      println(
        s"Start Processing Agg Level ${aLevel.level}, Grain: ${grainEnum}, outputPath: ${outputDataSet.datasetPath}, at ${java.time.Instant.now().toString}"
      )
      // we don't need aggregate on empty data
      val aggInputDf = inputDf.filter(shouldTrackTDID(col(aLevel.level)))

      val result = aggByDefinition(aggInputDf, initialAggDef, aLevel)
      outputDataSet.writeWithRowCountLog(result, aLevel.initWritePartitions)
      aggInputDf.unpersist()
      result.unpersist()
      println(
        s"End Processing Agg Level ${aLevel.level}, Grain: ${grainEnum}, outputPath: ${outputDataSet.datasetPath}, at ${java.time.Instant.now().toString}"
      )
    }
  }

  // aggregate from raw data source base on definition
  def aggByDefinition(
                       inputDf: Dataset[_],
                       aggDef: AggDefinition,
                       aLevel: AggLevelConfig
                     ): Dataset[_] = {
    val aggCols = genAggCols(aggDef)
    if (aggCols.isEmpty) throw new IllegalArgumentException(s"No aggregation functions found for level ${aLevel.level}")

    val allAggCols =
      if (aLevel.enableFeatureKeyCount)
        genKeyCountCol(aLevel.level) ++ aggCols
      else aggCols

    val mergeCols =
      if (aLevel.enableFeatureKeyCount)
        genKeyCountMergeCol() ++ genMergeCols(aggDef)
      else genMergeCols(aggDef)

    if (aLevel.saltSize > 1) {
      inputDf
        .withColumn("random", (rand() * aLevel.saltSize).cast("int"))
        .groupBy(col(aLevel.level), col("random"))
        .agg(allAggCols.head, allAggCols.tail: _*)
        .groupBy(col(aLevel.level))
        .agg(mergeCols.head, mergeCols.tail: _*)
        .withColumnRenamed(aLevel.level, "FeatureKey")
    } else {
      inputDf
        .groupBy(col(aLevel.level))
        .agg(allAggCols.head, allAggCols.tail: _*)
        .withColumnRenamed(aLevel.level, "FeatureKey")
    }
  }

  private def genAggCols(aggDef: AggDefinition): Array[Column] = {
    if (aggDef.aggregations.isEmpty) return Array.empty[Column]

    aggDef.aggregations
      .flatMap(fieldAggSpec => {
        if (fieldAggSpec.aggFuncs.isEmpty) return Array.empty[Column]

        fieldAggSpec.aggFuncs.map { func =>
          val processor = AggFuncProcessorFactory.getProcessor(func, fieldAggSpec)
          processor.aggCol().alias(AggFuncProcessorUtils.getColNameByFunc(func, fieldAggSpec))
        }
      })
      .toArray
  }
}
