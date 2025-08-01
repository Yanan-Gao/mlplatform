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
import com.thetradedesk.featurestore.transform.TransformFactory
import com.thetradedesk.spark.TTDSparkContext.spark
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{Column, DataFrame, Dataset}

import scala.collection.mutable

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
      "year" -> date.getYear.toString,
      "monthMM" -> f"${date.getMonthValue}%02d",
      "dayDD" -> f"${date.getDayOfMonth}%02d",
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
      // do merge if there is merge result from previous grain
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

  /**
   * @param initialAggDef
   * @param overridesMap
   * @param aggLevelsToProcess
   *
   * 1. Group aggregation at each level: {mainAggLevel: [derivations]}. For example, since SeedId is derived from TDID, first aggregate by TDID, then use that result to generate data at the SeedId level.
   * 2. Filter the mainAggLevel and derivations that has been processed
   * 3. Load datasource once only if at least one mainAggLevel required
   * 4. Directly read mainAggLevel agg result if it's processed to avoid duplicate agg flow
   */
  private def loadAndProcess(
                              initialAggDef: AggDefinition,
                              overridesMap: Map[String, String],
                              aggLevelCandidates: Seq[AggLevelConfig]
                            ): Unit = {
    if (aggLevelCandidates.isEmpty) return

    val profileDataSetByLevel = createProfileDatasets(initialAggDef, overridesMap, aggLevelCandidates)
    val aggJobGraph = buildAggJobGraph(aggLevelCandidates, profileDataSetByLevel)
    val dataSource = loadDataSourceIfNeeded(initialAggDef, overridesMap, aggLevelCandidates, aggJobGraph, profileDataSetByLevel)

    processAggregations(initialAggDef, overridesMap, aggLevelCandidates, aggJobGraph, profileDataSetByLevel, dataSource)
  }

  private def createProfileDatasets(
                                     initialAggDef: AggDefinition,
                                     overridesMap: Map[String, String],
                                     aggLevelCandidates: Seq[AggLevelConfig]
                                   ): Map[String, ProfileDataset] = {
    aggLevelCandidates.map { candidate =>
      val overrides = overridesMap + ("indexPartition" -> candidate.level)
      candidate.level -> ProfileDataset(
        rootPath = initialAggDef.initAggConfig.outputRootPath,
        prefix = initialAggDef.initAggConfig.outputPrefix,
        grain = Some(grainEnum),
        overrides = overrides
      )
    }.toMap
  }

  private def buildAggJobGraph(
                                aggLevelCandidates: Seq[AggLevelConfig],
                                profileDataSetByLevel: Map[String, ProfileDataset]
                              ): Map[String, Set[String]] = {
    val rawAggJobGraph = mutable.Map.empty[String, mutable.Set[String]]

    aggLevelCandidates.foreach { levelInfo =>
      levelInfo.dependency match {
        case None =>
          rawAggJobGraph.getOrElseUpdate(levelInfo.level, mutable.Set.empty)

        case Some(_) if shouldProcessLevel(levelInfo, profileDataSetByLevel) =>
          val derivations = rawAggJobGraph.getOrElseUpdate(levelInfo.dependency.get, mutable.Set.empty)
          derivations += levelInfo.level

        case _ =>
          println(s"Skipping Derived Agg Level ${levelInfo.level}, already processed at: ${profileDataSetByLevel(levelInfo.level).datasetPath}")
      }
    }

    rawAggJobGraph
      .filter { case (upLevelKey, derivations) =>
        val needProcess = shouldProcessTopLevel(upLevelKey, derivations, profileDataSetByLevel)
        if (!needProcess) {
          println(s"Skipping Top Agg Level $upLevelKey, Grain: $grainEnum, Dataset existed in outputPath: ${profileDataSetByLevel(upLevelKey).datasetPath}")
        }
        needProcess
      }
      .map { case (k, v) => k -> v.toSet }
      .toMap
  }

  private def shouldProcessLevel(levelInfo: AggLevelConfig, profileDataSetByLevel: Map[String, ProfileDataset]): Boolean =
    overrideOutput || !profileDataSetByLevel(levelInfo.level).isProcessed

  private def shouldProcessTopLevel(upLevelKey: String, derivations: mutable.Set[String], profileDataSetByLevel: Map[String, ProfileDataset]): Boolean =
    overrideOutput || !profileDataSetByLevel(upLevelKey).isProcessed || derivations.nonEmpty

  private def loadDataSourceIfNeeded(
                                      initialAggDef: AggDefinition,
                                      overridesMap: Map[String, String],
                                      aggLevelCandidates: Seq[AggLevelConfig],
                                      aggJobGraph: Map[String, Set[String]],
                                      profileDataSetByLevel: Map[String, ProfileDataset]
                                    ): DataFrame = {
    if (aggJobGraph.keys.exists(key => !profileDataSetByLevel(key).isProcessed) || overrideOutput) {
      val levelsToProcess = aggLevelCandidates.filter(candidate => aggJobGraph.contains(candidate.level))
      loadDataSource(initialAggDef, overridesMap, levelsToProcess)
    } else {
      spark.emptyDataFrame
    }
  }

  private def processAggregations(
                                   initialAggDef: AggDefinition,
                                   overridesMap: Map[String, String],
                                   aggLevelCandidates: Seq[AggLevelConfig],
                                   aggJobGraph: Map[String, Set[String]],
                                   profileDataSetByLevel: Map[String, ProfileDataset],
                                   dataSource: DataFrame
                                 ): Unit = {
    aggJobGraph.foreach { case (topLevel, derivations) =>
      val topLevelConfig = findLevelConfig(aggLevelCandidates, topLevel)
      val upLevelAggResult = processTopLevelAgg(initialAggDef, topLevelConfig, profileDataSetByLevel(topLevel), dataSource)

      processDerivedLevels(initialAggDef, overridesMap, aggLevelCandidates, derivations, profileDataSetByLevel, upLevelAggResult)

      upLevelAggResult.unpersist()
    }
  }

  private def findLevelConfig(aggLevelCandidates: Seq[AggLevelConfig], level: String): AggLevelConfig =
    aggLevelCandidates.find(_.level == level).get

  private def processTopLevelAgg(
                                  initialAggDef: AggDefinition,
                                  topLevelConfig: AggLevelConfig,
                                  profileDataset: ProfileDataset,
                                  dataSource: DataFrame
                                ): DataFrame = {
    if (overrideOutput || !profileDataset.isProcessed) {
      logProcessingStart(topLevelConfig.level, profileDataset.datasetPath)
      val filteredDf = dataSource.filter(shouldTrackTDID(col(topLevelConfig.level)))
      val result = aggByDefinition(filteredDf, initialAggDef, topLevelConfig).toDF()
      profileDataset.writeWithRowCountLog(result, topLevelConfig.initWritePartitions)
      logProcessingComplete(topLevelConfig.level, profileDataset.datasetPath)
      result
    } else {
      println(s"Start Reading Agg Level ${topLevelConfig.level} to process derivations, Grain: $grainEnum, outputPath: ${profileDataset.datasetPath}")
      profileDataset.readDataSet()
    }
  }

  private def processDerivedLevels(
                                    initialAggDef: AggDefinition,
                                    overridesMap: Map[String, String],
                                    aggLevelCandidates: Seq[AggLevelConfig],
                                    derivations: Set[String],
                                    profileDataSetByLevel: Map[String, ProfileDataset],
                                    upLevelAggResult: DataFrame
                                  ): Unit = {
    derivations.foreach { dLevel =>
      val derivedLevel = findLevelConfig(aggLevelCandidates, dLevel)
      val profileDataset = profileDataSetByLevel(dLevel)

      logProcessingStart(dLevel, profileDataset.datasetPath)

      val transformedResult = applyTransformations(derivedLevel, overridesMap, upLevelAggResult)
      val derivedResult = mergeByDefinition(transformedResult.toDF(), initialAggDef, derivedLevel)

      profileDataset.writeWithRowCountLog(derivedResult, derivedLevel.initWritePartitions)
      transformedResult.unpersist()
      derivedResult.unpersist()

      logProcessingComplete(dLevel, profileDataset.datasetPath)
    }
  }

  private def applyTransformations(
                                    derivedLevel: AggLevelConfig,
                                    overridesMap: Map[String, String],
                                    upLevelAggResult: DataFrame
                                  ): DataFrame = {
    derivedLevel.transform.fold(upLevelAggResult) { transform =>
      val transformers = TransformFactory.getTransformers(transform, derivedLevel.transformContext ++ overridesMap)
      val result = transformers.foldLeft(upLevelAggResult)((df, transformer) => transformer.transform(df.toDF()))
      result.persist()
    }
  }

  private def logProcessingStart(level: String, datasetPath: String): Unit =
    println(s"Start Processing Agg Level $level, Grain: $grainEnum, outputPath: $datasetPath, at ${java.time.Instant.now()}")

  private def logProcessingComplete(level: String, datasetPath: String): Unit =
    println(s"Completed Processing Agg Level $level, Grain: $grainEnum, outputPath: $datasetPath, at ${java.time.Instant.now()}")

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
