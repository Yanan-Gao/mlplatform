package com.thetradedesk.featurestore.jobs

import com.thetradedesk.featurestore._
import com.thetradedesk.featurestore.configs.{AggDefinition, FieldAggSpec}
import com.thetradedesk.featurestore.constants.FeatureConstants.ColFeatureKey
import com.thetradedesk.featurestore.datasets.ProfileDataset
import com.thetradedesk.featurestore.features.Features.AggFunc.AggFunc
import com.thetradedesk.featurestore.features.Features._
import com.thetradedesk.featurestore.jobs.AggAttributions.sourcePartition
import com.thetradedesk.featurestore.transform.DescriptionAgg.DescMergeAggregator
import com.thetradedesk.featurestore.transform.FrequencyAgg.FrequencyMergeAggregator
import com.thetradedesk.featurestore.transform.Merger.joinDataFrames
import com.thetradedesk.featurestore.transform.VectorDescriptionAgg._
import com.thetradedesk.featurestore.utils.{PathUtils, StringUtils}
import com.thetradedesk.geronimo.shared.paddedDatePart
import com.thetradedesk.spark.TTDSparkContext
import com.thetradedesk.spark.TTDSparkContext.spark
import com.thetradedesk.spark.util.io.FSUtils
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{Column, DataFrame, Dataset}


// This job can be run hourly/daily, do the rollup agg from initial agg result
// 1. fetch the agg definition from config
// 2. check if the agg result is processed
// 3. if not processed, load the initial agg result
// 4. do the rollup agg
// 5. write the result to the output path
object RollupAggJob extends FeatureStoreBaseJob {
  override def jobName: String = s"${getClass.getSimpleName.stripSuffix("$")}"

  override def runTransform(args: Array[String]): Array[(String, Long)] = {
    val aggDef = AggDefinition.loadConfig(dataSource = Some(aggDataSource))
    aggDef.aggLevels.foreach(aLevel => {
      val baseOverrides = Map(
        "sourcePartition" -> sourcePartition,
        "jobName" -> jobName,
        "indexPartition" -> aLevel,
        "dateStr" -> getDateStr(date),
        "ttdEnv" -> ttdEnv
      )

      loadAndProcess(aggDef, baseOverrides, aLevel)
    })

    Array(("BaseAgg" + aggDataSource, 0))
  }

  private def loadAndProcess(aggDef: AggDefinition, overridesMap: Map[String, String], aLevel: String): Unit = {
    val outputDataSet = ProfileDataset(
      rootPath = aggDef.outputRootPath,
      prefix = aggDef.outputPrefix,
      overrides = overridesMap
    )

    if (!overrideOutput && outputDataSet.isProcessed) {
      println(s"ProfileDataSet ${outputDataSet.datasetPath} existed, skip processing " +
        s"source ${sourcePartition}, aggLevel ${aLevel}, aggregation job ${jobName}")
    } else {
      // get a set of all aggregation windows
      val aggWindowSet = aggDef.aggregations.flatMap(_.aggWindows).toSet
      println(s"Processing Rollup Agg for DataSource ${aggDataSource}, aggLevel ${aLevel}, aggregation windows: ${aggWindowSet.mkString(", ")}")

      var cachedResults: Seq[Dataset[_]] = Seq.empty
      var resDf: Dataset[_] = null

      try {
        cachedResults = aggWindowSet.map(window => {
          println(s"Processing Rollup Agg for DataSource ${aggDataSource}, aggLevel ${aLevel}, window $window...")
          val inputDf = loadDataSource(aggDef, overridesMap, window)
          val result = aggByDefinition(inputDf, aggDef, window).cache() // Cache the result to avoid recomputation
          // Release the input data source after aggregation
          inputDf.unpersist()
          result
        }).toSeq
        resDf = cachedResults.reduceLeft((df1: Dataset[_], df2: Dataset[_]) => joinDataFrames(df1, df2))

        outputDataSet.writeWithRowCountLog(resDf)
      } finally {
        // Clean up all cached DataFrames after writing
        cachedResults.foreach(_.unpersist())
        if (resDf != null) {
          resDf.unpersist()
        }
      }
    }
  }

  private def loadDataSource(aggDef: AggDefinition, overrides: Map[String, String], window: Int): DataFrame = {
    val initOutputPrefix = StringUtils.applyNamedFormat(PathUtils.concatPath(aggDef.outputRootPath, aggDef.initOutputPrefix), overrides)
    val paths = (0 until window)
      .map(i => PathUtils.concatPath(initOutputPrefix, s"date=${paddedDatePart(date.minusDays(i))}"))
      .filter(FSUtils.directoryExists(_)(TTDSparkContext.spark))
    if (paths.isEmpty) {
      throw new RuntimeException(s"No paths of initial agg result found for window $window, overrides = $overrides")
    }
    spark.read.option("basePath", initOutputPrefix).parquet(paths: _*)
  }

  def aggByDefinition(
                       inputDf: DataFrame,
                       aggDef: AggDefinition,
                       window: Int
                     ): DataFrame = {
    val mergeCols = genMergeCols(aggDef)

    val aggResult = inputDf
      .groupBy(col(ColFeatureKey))
      .agg(mergeCols.head, mergeCols.tail: _*)

    var columnsToKeep = Seq(col(ColFeatureKey))

    val expandResult = aggDef.aggregations.filter(x => x.aggWindows.contains(window)).foldLeft(aggResult) {
      (df, fieldAggSpec) =>
        fieldAggSpec.aggFuncs.foldLeft(df) { (currentDf: DataFrame, func: AggFunc) =>
          var newCol = s"${func}_${fieldAggSpec.field}_Last${window}${fieldAggSpec.getWindowSuffix}"
          if (func == AggFunc.TopN) {
            newCol = s"Top${fieldAggSpec.topN}_${fieldAggSpec.field}_Last${window}${fieldAggSpec.getWindowSuffix}"
          }
          columnsToKeep :+= col(newCol)
          func match {
            case AggFunc.Sum => currentDf.withColumn(newCol,
              when(col(s"${fieldAggSpec.field}_Desc").getItem("allNull") === 1, lit(null))
                .otherwise(col(s"${fieldAggSpec.field}_Desc").getItem("sum").cast("double")))
            case AggFunc.Count => currentDf.withColumn(newCol,
              when(col(s"${fieldAggSpec.field}_Desc").getItem("allNull") === 1, lit(null))
                .otherwise(col(s"${fieldAggSpec.field}_Desc").getItem("count").cast("double")))
            case AggFunc.Mean => currentDf.withColumn(newCol,
              when(col(s"${fieldAggSpec.field}_Desc").getItem("allNull") === 1, lit(null))
                .otherwise(getMean(col(s"${fieldAggSpec.field}_Desc"))))
            case AggFunc.NonZeroMean => currentDf.withColumn(newCol,
              when(col(s"${fieldAggSpec.field}_Desc").getItem("allNull") === 1, lit(null))
                .otherwise(getNonZeroMean(col(s"${fieldAggSpec.field}_Desc"))))
            case AggFunc.NonZeroCount => currentDf.withColumn(newCol,
              when(col(s"${fieldAggSpec.field}_Desc").getItem("allNull") === 1, lit(null))
                .otherwise(col(s"${fieldAggSpec.field}_Desc").getItem("nonZeroCount").cast("double")))
            case AggFunc.Min => currentDf.withColumn(newCol,
              when(col(s"${fieldAggSpec.field}_Desc").getItem("allNull") === 1, lit(null))
                .otherwise(col(s"${fieldAggSpec.field}_Desc").getItem("min").cast("double")))
            case AggFunc.Max => currentDf.withColumn(newCol,
              when(col(s"${fieldAggSpec.field}_Desc").getItem("allNull") === 1, lit(null))
                .otherwise(col(s"${fieldAggSpec.field}_Desc").getItem("max").cast("double")))
            case AggFunc.Frequency => currentDf.withColumnRenamed(s"${fieldAggSpec.field}_${func}", newCol)
            case AggFunc.TopN =>
              currentDf.withColumn(newCol, getTopN(fieldAggSpec.topN, col(s"${fieldAggSpec.field}_${AggFunc.Frequency}"))) // retrieve top N from frequency
            case AggFunc.VectorMean =>
              currentDf.withColumn(newCol, getMeanVector(fieldAggSpec.arraySize, col(s"${fieldAggSpec.field}_${AggFunc.VectorDesc}")))
            case AggFunc.VectorMax =>
              currentDf.withColumn(newCol, getDescVectorByIndex(IndexMaxs, col(s"${fieldAggSpec.field}_${AggFunc.VectorDesc}")))
            case AggFunc.VectorMin =>
              currentDf.withColumn(newCol, getDescVectorByIndex(IndexMins, col(s"${fieldAggSpec.field}_${AggFunc.VectorDesc}")))
            case AggFunc.VectorSum =>
              currentDf.withColumn(newCol, getDescVectorByIndex(IndexSums, col(s"${fieldAggSpec.field}_${AggFunc.VectorDesc}")))
            case AggFunc.VectorCount =>
              currentDf.withColumn(newCol, getDescVectorByIndex(IndexCounts, col(s"${fieldAggSpec.field}_${AggFunc.VectorDesc}")))
            case _ => throw new RuntimeException(s"Unsupported aggFunc ${func} for field ${fieldAggSpec.field}")
          }
        }
    }

    expandResult.select(columnsToKeep: _*)
  }

  private def getDescVectorByIndex(index: Int, column: Column): Column = {
    val maxVectorUdf = udf[Option[Seq[Double]], Seq[Seq[Double]]]((vector: Seq[Seq[Double]]) => {
      if (vector == null || vector.isEmpty) {
        None
      } else {
        Some(vector(index))
      }
    })
    maxVectorUdf(column)
  }

  private def getMeanVector(arraySize: Int, column: Column): Column = {

    val meanVectorUdf = udf[Option[Seq[Double]], Seq[Seq[Double]]]((vector: Seq[Seq[Double]]) => {
      if (vector == null || vector.isEmpty) {
        None
      } else {
        Some(vector(IndexSums).zip(vector(IndexCounts)).map(x => x._1 / x._2))
      }
    })
    meanVectorUdf(column)
  }

  private def getTopN(topN: Int, column: Column): Column = {
    // Create a UDF to sort map by count and extract top N keys
    val topNUdf = udf[Option[Seq[String]], Map[String, Int]]((map: Map[String, Int]) => {
      if (map == null || map.isEmpty) {
        None
      } else {
        Some(map.toSeq
          .sortBy(-_._2) // Sort by count in descending order
          .take(topN)
          .map(_._1)) // Extract only the keys
      }
    })

    topNUdf(column)
  }

  private def getMean(column: Column): Column = {
    column.getItem("sum").cast("double") /
      column.getItem("count").cast("double")
  }

  private def getNonZeroMean(column: Column): Column = {
    column.getItem("sum").cast("double") /
      column.getItem("nonZeroCount").cast("double")
  }

  private def genMergeCols(aggDef: AggDefinition): Seq[Column] = {
    if (aggDef.aggregations.isEmpty) Array.empty[Column]
    else aggDef.aggregations.flatMap(genAggMergeCol)
  }

  private def genAggMergeCol(fieldAggSpec: FieldAggSpec): Seq[Column] = {
    if (fieldAggSpec.aggFuncs.isEmpty) {
      Array.empty[Column]
    }
    val initialAggSpec = fieldAggSpec.extractFieldAggSpec(fieldAggSpec.windowUnit)

    initialAggSpec.aggFuncs.map { func =>
      val column = func match {
        case AggFunc.Desc => genDescMergeCol(fieldAggSpec, func)
        case AggFunc.Frequency => genFrequencyMergeCol(fieldAggSpec, func)
        case AggFunc.TopN => genFrequencyMergeCol(fieldAggSpec, func)
        case AggFunc.VectorDesc => genVectorDescMergeCol(fieldAggSpec, func)
        case _ => throw new RuntimeException(s"Unsupported aggFunc ${func} for field ${fieldAggSpec.field}")
      }
      column.alias(s"${fieldAggSpec.field}_${func}")
    }
  }

  private def genFrequencyMergeCol(spec: FieldAggSpec, func: AggFunc): Column = {
    val descUdaf = udaf(new FrequencyMergeAggregator(spec.topN))
    descUdaf(col(s"${spec.field}_${func}"))
  }

  private def genDescMergeCol(spec: FieldAggSpec, func: AggFunc): Column = {
    val descUdaf = udaf(new DescMergeAggregator())
    descUdaf(col(s"${spec.field}_${func}"))
  }

  private def genVectorDescMergeCol(spec: FieldAggSpec, func: AggFunc): Column = {
    val descUdaf = udaf(new VectorDescMergeAggregator(spec.arraySize))
    descUdaf(col(s"${spec.field}_${func}"))
  }
}

