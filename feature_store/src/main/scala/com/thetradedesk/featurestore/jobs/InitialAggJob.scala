package com.thetradedesk.featurestore.jobs

import com.thetradedesk.featurestore._
import com.thetradedesk.featurestore.configs.{AggDefinition, FieldAggSpec}
import com.thetradedesk.featurestore.datasets.{BidContextualRecordDataset, ProfileDataset}
import com.thetradedesk.featurestore.features.Features.AggFunc.AggFunc
import com.thetradedesk.featurestore.features.Features._
import com.thetradedesk.featurestore.jobs.AggAttributions.sourcePartition
import com.thetradedesk.featurestore.transform.DescriptionAgg.{ArrayDescAggregator, DescAggregator, DescMergeAggregator}
import com.thetradedesk.featurestore.transform.FrequencyAgg.{ArrayFrequencyAggregator, FrequencyAggregator, FrequencyMergeAggregator}
import com.thetradedesk.featurestore.transform.VectorDescriptionAgg.{VectorDescAggregator, VectorDescMergeAggregator}
import com.thetradedesk.featurestore.utils.StringUtils
import com.thetradedesk.spark.TTDSparkContext.spark
import org.apache.spark.sql.functions.{transform, _}
import org.apache.spark.sql.types.{DoubleType, StringType}
import org.apache.spark.sql.{Column, Dataset}


// This job can be run hourly/daily, do the base agg from datasource configuration
object InitialAggJob extends FeatureStoreBaseJob {
  override def jobName: String = s"${getClass.getSimpleName.stripSuffix("$")}"

  override def runTransform(args: Array[String]): Array[(String, Long)] = {
    val aggDef = AggDefinition.loadConfig(dataSource = Some(aggDataSource))
    val initialAggDef = aggDef.extractInitialAggDefinition()
    val baseOverrides = Map(
      "sourcePartition" -> sourcePartition,
      "jobName" -> jobName,
      "indexPartition" -> aggLevel,
      "dateStr" -> getDateStr(date),
      "ttdEnv" -> ttdEnv
    )

    if (aggDef.grain == "hour") {
      hourArray.foreach { hour =>
        val overrides = baseOverrides + ("hourInt" -> hour.toString)
        loadAndProcess(initialAggDef, overrides)
      }
    } else {
      loadAndProcess(initialAggDef, baseOverrides)
    }

    Array(("BaseAgg" + aggDataSource, 0))
  }

  private def loadAndProcess(initialAggDef: AggDefinition, overridesMap: Map[String, String]): Unit = {
    val outputDataSet = ProfileDataset(
      rootPath = initialAggDef.outputRootPath,
      prefix = initialAggDef.initOutputPrefix,
      grain = Some(initialAggDef.grain),
      overrides = overridesMap
    )

    if (!overrideOutput && outputDataSet.isProcessed) {
      println(s"ProfileDataSet ${outputDataSet.datasetPath} existed, skip processing " +
        s"source ${sourcePartition}, aggLevel ${aggLevel}, aggregation job ${jobName}")
    } else {
      val inputDf = loadDataSource(initialAggDef, overridesMap)
        .withColumnRenamed("UIID", "TDID")
        .filter(shouldTrackTDID(col(aggLevel)))

      val result = aggByDefinition(inputDf, aggLevel, initialAggDef)
      outputDataSet.writeWithRowCountLog(result)
    }
  }

  private def loadDataSource(aggDef: AggDefinition, overrides: Map[String, String]): Dataset[_] = {
    BidContextualRecordDataset(
      aggDef.rootPath,
      StringUtils.applyNamedFormat(aggDef.prefix, overrides)
    ).readDatasetFromPath(Some(aggDef.format))
  }

  def aggByDefinition(
                       inputDf: Dataset[_],
                       aggLevel: String,
                       aggDef: AggDefinition,
                       saltSize: Int = saltSize
                     ): Dataset[_] = {
    val aggCols = genAggCols(aggDef)
    val mergeCols = genMergeCols(aggDef)

    inputDf
      .withColumn("random", (rand() * saltSize).cast("int"))
      .groupBy(col(aggLevel), col("random"))
      .agg(aggCols.head, aggCols.tail: _*)
      .groupBy(col(aggLevel))
      .agg(mergeCols.head, mergeCols.tail: _*)
      .withColumnRenamed(aggLevel, "FeatureKey")
  }

  private def genMergeCols(aggDef: AggDefinition): Seq[Column] = {
    if (aggDef.aggregations.isEmpty) Array.empty[Column]
    else aggDef.aggregations.flatMap(genAggMergeCol)
  }

  private def genAggMergeCol(fieldAggSpec: FieldAggSpec): Seq[Column] = {
    if (fieldAggSpec.aggFuncs.isEmpty) Array.empty[Column]
    else fieldAggSpec.aggFuncs.map { func =>
      val column = func match {
        case AggFunc.Desc => genDescMergeCol(fieldAggSpec, func)
        case AggFunc.VectorDesc => genVectorDescMergeCol(fieldAggSpec, func)
        case AggFunc.Frequency => genFrequencyMergeCol(fieldAggSpec, func)
        case AggFunc.TopN => genFrequencyMergeCol(fieldAggSpec, func)
        case _ => throw new RuntimeException(s"Unsupported aggFunc ${func} for field ${fieldAggSpec.field}")
      }
      column.alias(s"${fieldAggSpec.field}_${func}")
    }
  }

  private def genFrequencyMergeCol(spec: FieldAggSpec, func: AggFunc): Column = {
    val descUdaf = udaf(new FrequencyMergeAggregator(spec.topN))
    descUdaf(col(s"${spec.field}_Temp_${func}"))
  }

  private def genDescMergeCol(spec: FieldAggSpec, func: AggFunc): Column = {
    val descUdaf = udaf(new DescMergeAggregator())
    descUdaf(col(s"${spec.field}_Temp_${func}"))
  }

  private def genVectorDescMergeCol(spec: FieldAggSpec, func: AggFunc): Column = {
    if (spec.arraySize < 1) throw new RuntimeException(s"Unsupported aggFunc ${func} for field ${spec.field}")
    val descUdaf = udaf(new VectorDescMergeAggregator(spec.arraySize))
    descUdaf(col(s"${spec.field}_Temp_${func}"))
  }

  private def genAggCols(aggDef: AggDefinition): Seq[Column] = {
    if (aggDef.aggregations.isEmpty) Array.empty[Column]
    else aggDef.aggregations.flatMap(genAggCol)
  }

  private def genAggCol(fieldAggSpec: FieldAggSpec): Seq[Column] = {
    if (fieldAggSpec.aggFuncs.isEmpty) Array.empty[Column]
    else fieldAggSpec.aggFuncs.map { func =>
      val column = func match {
        case AggFunc.Desc => genDescCol(fieldAggSpec)
        case AggFunc.VectorDesc => genVectorDescCol(fieldAggSpec)
        case AggFunc.Frequency => genFrequencyCol(fieldAggSpec)
        case AggFunc.TopN => genFrequencyCol(fieldAggSpec)
        case _ => throw new RuntimeException(s"Unsupported aggFunc ${func} for field ${fieldAggSpec.field}")
      }
      column.alias(s"${fieldAggSpec.field}_Temp_${func}")
    }
  }

  private def genDescCol(fieldAggSpec: FieldAggSpec): Column = {
    if (fieldAggSpec.dataType.startsWith("array")) {
      val doubleArray = transform(col(fieldAggSpec.field), num => num.cast(DoubleType))
      val descUdaf = udaf(new ArrayDescAggregator())
      descUdaf(doubleArray)
    } else {
      val descUdaf = udaf(new DescAggregator())
      descUdaf(col(fieldAggSpec.field).cast(DoubleType))
    }
  }

  private def genVectorDescCol(fieldAggSpec: FieldAggSpec): Column = {
    if (!fieldAggSpec.dataType.startsWith("array")) throw new RuntimeException(s"Unsupported aggFunc ${fieldAggSpec.aggFuncs} for field ${fieldAggSpec.field}")
    if (fieldAggSpec.arraySize < 1) throw new RuntimeException(s"Unsupported arraySize ${fieldAggSpec.arraySize} for aggFunc ${fieldAggSpec.aggFuncs} for field ${fieldAggSpec.field}")

    val doubleArray = transform(col(fieldAggSpec.field), num => num.cast(DoubleType))
    val descUdaf = udaf(new VectorDescAggregator(fieldAggSpec.arraySize))
    descUdaf(doubleArray)
  }

  private def genFrequencyCol(fieldAggSpec: FieldAggSpec): Column = {
    if (fieldAggSpec.dataType.startsWith("array")) {
      val strArray = transform(col(fieldAggSpec.field), num => num.cast("string"))
      val seqTopCol = udaf(new ArrayFrequencyAggregator(fieldAggSpec.topN))
      seqTopCol(strArray)
    } else {
      val seqTopCol = udaf(new FrequencyAggregator(fieldAggSpec.topN))
      seqTopCol(col(fieldAggSpec.field).cast(StringType))
    }
  }
}
