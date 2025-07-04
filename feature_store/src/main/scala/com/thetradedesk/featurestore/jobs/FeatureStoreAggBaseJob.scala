package com.thetradedesk.featurestore.jobs

import com.thetradedesk.featurestore._
import com.thetradedesk.featurestore.aggfunctions.AggFuncProcessorFactory
import com.thetradedesk.featurestore.configs.{AggDefinition, AggLevelConfig, AggTaskConfig}
import com.thetradedesk.featurestore.constants.FeatureConstants.ColFeatureKeyCount
import com.thetradedesk.featurestore.rsm.CommonEnums.DataIntegrity
import com.thetradedesk.featurestore.utils.{PathUtils, StringUtils}
import com.thetradedesk.geronimo.shared.paddedDatePart
import com.thetradedesk.spark.TTDSparkContext
import com.thetradedesk.spark.TTDSparkContext.spark
import com.thetradedesk.spark.util.io.FSUtils
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.LongType
import org.apache.spark.sql.{Column, DataFrame}

abstract class FeatureStoreAggBaseJob extends FeatureStoreBaseJob {

  /**
   * Load data for a window of consecutive days (can be single day or multiple days)
   *
   * @param aggDef        Aggregation definition
   * @param overridesMap  Overrides map
   * @param startDaysBack Starting number of days back (inclusive)
   * @param endDaysBack   Ending number of days back (inclusive)
   * @return DataFrame for the specified days
   */
  protected def loadInitialAggDataWindow(initAggConfig: AggTaskConfig, overridesMap: Map[String, String], startDaysBack: Int = 0, endDaysBack: Int = 0): DataFrame = {
    val initOutputPrefix = StringUtils.applyNamedFormat(PathUtils.concatPath(initAggConfig.outputRootPath, initAggConfig.outputPrefix), overridesMap)

    // Generate all required paths
    val allPaths = (startDaysBack to endDaysBack).map { daysBack =>
      val datePart = paddedDatePart(date.minusDays(daysBack), datePrefix = Some("date="))
      val path = PathUtils.concatPath(initOutputPrefix, datePart)
      (daysBack, datePart, path)
    }
    println(s"Loading data of window: [${paddedDatePart(date.minusDays(startDaysBack))}, ${paddedDatePart(date.minusDays(endDaysBack))}]")


    // Check which paths exist and which are missing
    val (existingPaths, missingPaths) = allPaths.partition { case (_, _, path) => FSUtils.directoryExists(path)(TTDSparkContext.spark) }

    // Print detailed information about missing paths
    if (missingPaths.nonEmpty) {
      println("  Missing data paths:")
      missingPaths.foreach { case (daysBack, datePart, path) =>
        println(s"    - Day ${daysBack} back ($datePart): $path")
      }
    }

    // Print information about existing paths
    if (existingPaths.nonEmpty) {
      println(s"  Found ${existingPaths.size} existing data paths:")
      existingPaths.foreach { case (daysBack, datePart, path) =>
        println(s"      + Day ${daysBack} back ($datePart): $path")
      }
    }


    // todo we should validate recursively for all sub director, it's ok now since we have airflow data senstor check
    if (initAggConfig.dataIntegrity == DataIntegrity.AllExist && existingPaths.size != allPaths.size) {
      throw new IllegalArgumentException(s"Some data are missing for window ${startDaysBack} to ${endDaysBack} days back. Base path: $initOutputPrefix")
    }

    if (existingPaths.isEmpty) {
      println(s"Warning: No existing data paths found for window ${startDaysBack} to ${endDaysBack} days back. Base path: $initOutputPrefix")
      throw new IllegalArgumentException(s"No existing data paths found for window ${startDaysBack} to ${endDaysBack} days back. Base path: $initOutputPrefix")
    }

    // Load only existing paths
    val pathsToLoad = existingPaths.map(_._3)
    spark.read.option("basePath", initOutputPrefix).parquet(pathsToLoad: _*).drop("date", "hour")
  }

  protected def loadDataSource(aggDef: AggDefinition, overridesMap: Map[String, String], filteredLevels: Seq[AggLevelConfig]): DataFrame = {
    val dataSourcePrefix = StringUtils.applyNamedFormat(PathUtils.concatPath(aggDef.dataSource.rootPath, aggDef.dataSource.prefix), overridesMap)
    if (FSUtils.directoryExists(dataSourcePrefix)(TTDSparkContext.spark)) {
      // Get the list of level columns we need
      val levelColumns = filteredLevels.map(_.level).distinct
      val fieldColumns = aggDef.aggregations.map(_.field).distinct

      // Read the data and rename UIID to TDID
      val df = spark.read
        .option("basePath", dataSourcePrefix)
        .parquet(dataSourcePrefix)
        .withColumnRenamed("UIID", "TDID")
        .select((levelColumns ++ fieldColumns).map(col): _*)

      // Filter out rows where all level columns are null
      val notAllNullFilter = fieldColumns.map(c => col(c).isNotNull).reduce(_ || _)
      df.filter(notAllNullFilter)
    } else {
      spark.emptyDataFrame
    }
  }


  protected def genMergeCols(aggDef: AggDefinition): Array[Column] = {
    if (aggDef.aggregations.isEmpty) Array.empty[Column]
    else aggDef.aggregations.flatMap(fieldAggSpec => {
      if (fieldAggSpec.aggFuncs.isEmpty) Array.empty[Column]
      else {
        fieldAggSpec.aggFuncs.map { func =>
          val processor = AggFuncProcessorFactory.getProcessor(fieldAggSpec.dataType, func)
          val column = processor.merge(fieldAggSpec)
          column.alias(s"${fieldAggSpec.field}_${func}")
        }
      }
    }).toArray
  }

  protected def genKeyCountMergeCol(): Array[Column] = {
    Array(sum(col(ColFeatureKeyCount)).cast(LongType).alias(ColFeatureKeyCount))
  }

}
