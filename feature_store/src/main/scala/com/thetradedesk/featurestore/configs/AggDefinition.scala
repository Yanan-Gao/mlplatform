package com.thetradedesk.featurestore.configs

import com.thetradedesk.featurestore.constants.FeatureConstants.{GrainDay, SingleUnitWindow}
import com.thetradedesk.featurestore.features.Features.AggFunc
import com.thetradedesk.featurestore.features.Features.AggFunc.AggFunc
import com.thetradedesk.featurestore.ttdEnv
import org.yaml.snakeyaml.Yaml

import scala.collection.JavaConverters._
import scala.collection.mutable
import scala.io.Source
import scala.util.{Failure, Success, Try}


case class AggDefinition(
                          datasource: String,
                          format: String,
                          rootPath: String,
                          prefix: String,
                          outputRootPath: String,
                          outputPrefix: String,
                          initOutputPrefix: String,
                          grain: String,
                          aggLevels: Set[String],
                          aggregations: Seq[FieldAggSpec]
                        ) {
  /**
   * Extracts initial aggregation definition for the given time grain
   * @return AggDefinition with initial aggregation specs
   */
  def extractInitialAggDefinition(): AggDefinition = {
    val initialAggregations = aggregations.map(_.extractFieldAggSpec(grain))
    
    copy(aggregations = initialAggregations)
  }
}

case class FieldAggSpec(
                         field: String,
                         dataType: String,
                         arraySize: Int = -1,
                         aggWindows: Seq[Int],
                         windowUnit: String,
                         topN: Int = -1,
                         aggFuncs: Seq[AggFunc]
                       ) {
  /**
   * Extracts initial aggregation specification for the given time unit
   * @param windowUnit Time unit for aggregation
   * @return FieldAggSpec with initial aggregation functions
   */
  def extractFieldAggSpec(windowUnit: String): FieldAggSpec = {
    val initialAggFunctions = new mutable.HashSet[AggFunc]()
    var initialTopN = -1

    aggFuncs.foreach(x => {
      val initFunc = AggFunc.getInitialAggFunc(x)
      initialAggFunctions += initFunc
      if (x == AggFunc.TopN) {
        initialTopN = topN * 100
      }
    })

    copy(
      aggWindows = SingleUnitWindow,
      windowUnit = windowUnit,
      topN = initialTopN,
      aggFuncs = initialAggFunctions.toSeq
    )
  }

  def getWindowSuffix: String = {
    windowUnit match {
      case GrainDay => "D"
      case _ => throw new IllegalArgumentException(s"Unsupported window unit: $windowUnit")
    }

  }
}

object AggDefinition {
  private val YAML_BASE_PATH = if (ttdEnv == "local") "/jobconfigsv2_local" else "/jobconfigsv2"

  def loadConfig(dataSource: Option[String] = None, filePath: Option[String] = None): AggDefinition = {
    require(dataSource.nonEmpty || filePath.nonEmpty, "Both dataSource and filePath are empty")

    val configString = dataSource match {
      case Some(ds) => loadFromClassPath(ds)
      case None => loadByFilePath(filePath.get)
    }

    val config = parseYamlConfig(configString)
    validateConfig(config)
    config
  }

  private def validateConfig(config: AggDefinition): Unit = {
    config.aggregations.foreach { agg =>
      agg.aggFuncs.foreach { func =>
        if (func == AggFunc.TopN && agg.topN < 1) {
          throw new IllegalArgumentException(s"TopN is not specified for field ${agg.field}")
        }

        if (AggFunc.isVectorFunc(func) && agg.arraySize < 1) {
          throw new IllegalArgumentException(s"Array size is not specified for field ${agg.field}")
        }
      }
    }
  }

  private def parseYamlConfig(configString: String): AggDefinition = {
    val yaml = new Yaml()
    
    Try {
      val javaMap = yaml.load[java.util.Map[String, Any]](configString)
      val yamlMap = javaMap.asScala.toMap

      val aggregations = parseAggregations(yamlMap)
      
      AggDefinition(
        datasource = yamlMap("datasource").toString,
        format = yamlMap.getOrElse("format", "parquet").toString,
        rootPath = yamlMap("rootPath").toString,
        prefix = yamlMap("prefix").toString,
        outputRootPath = yamlMap("outputRootPath").toString,
        outputPrefix = yamlMap("outputPrefix").toString,
        initOutputPrefix = yamlMap("initOutputPrefix").toString,
        grain = yamlMap("grain").toString,
        aggLevels = yamlMap("aggLevels").asInstanceOf[java.util.List[String]].asScala.toSet,
        aggregations = aggregations
      )
    } match {
      case Success(config) => config
      case Failure(e) => 
        throw new IllegalArgumentException(s"Failed to parse YAML configuration: $configString", e)
    }
  }

  private def parseAggregations(yamlMap: Map[String, Any]): Seq[FieldAggSpec] = {
    yamlMap("aggregations")
      .asInstanceOf[java.util.List[java.util.Map[String, Any]]]
      .asScala
      .map { agg =>
        val scalaAgg = agg.asScala.toMap
        val aggField = scalaAgg("field").toString
        
        FieldAggSpec(
          field = aggField,
          dataType = scalaAgg("dataType").toString,
          arraySize = scalaAgg.getOrElse("arraySize", -1).asInstanceOf[Int],
          aggWindows = scalaAgg("aggWindows").asInstanceOf[java.util.List[Int]].asScala,
          windowUnit = scalaAgg("windowUnit").toString,
          topN = scalaAgg.getOrElse("topN", -1).asInstanceOf[Int],
          aggFuncs = parseAggFunctions(scalaAgg, aggField)
        )
      }
  }

  private def parseAggFunctions(scalaAgg: Map[String, Any], aggField: String): Seq[AggFunc] = {
    scalaAgg("aggFunc")
      .asInstanceOf[java.util.List[String]]
      .asScala
      .map { str =>
        AggFunc.fromString(str).getOrElse(
          throw new IllegalArgumentException(s"Unable to parse aggregation function: $str, for field $aggField")
        )
      }
  }

  private def loadByFilePath(filePath: String): String = {
    Try {
      val inputStream = new java.io.FileInputStream(filePath)
      if (inputStream == null) {
        throw new IllegalArgumentException(s"YAML file not found: $filePath")
      }
      Source.fromInputStream(inputStream).mkString
    }.getOrElse(throw new IllegalArgumentException(s"Failed to load YAML file: $filePath"))
  }

  private def loadFromClassPath(dataSource: String): String = {
    val yamlPath = s"$YAML_BASE_PATH/Agg${dataSource}.yml"
    Try {
      val inputStream = getClass.getResourceAsStream(yamlPath)
      if (inputStream == null) {
        throw new IllegalArgumentException(s"YAML file not found: $yamlPath")
      }
      Source.fromInputStream(inputStream).mkString
    }.getOrElse(throw new IllegalArgumentException(s"Failed to load YAML from classpath: $yamlPath"))
  }
}