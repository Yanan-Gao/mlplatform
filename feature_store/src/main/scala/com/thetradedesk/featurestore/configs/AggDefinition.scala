package com.thetradedesk.featurestore.configs

import com.thetradedesk.featurestore.constants.FeatureConstants.SingleUnitWindow
import com.thetradedesk.featurestore.features.Features.AggFunc
import com.thetradedesk.featurestore.features.Features.AggFunc.AggFunc
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

    aggFuncs.foreach {
      case func @ (AggFunc.Sum | AggFunc.Count | AggFunc.NonZeroCount | 
                  AggFunc.Desc | AggFunc.Min | AggFunc.Max | 
                  AggFunc.Mean | AggFunc.NonZeroMean) =>
        initialAggFunctions += AggFunc.Desc

      case AggFunc.Frequency =>
        initialAggFunctions += AggFunc.Frequency

      case AggFunc.TopN =>
        initialAggFunctions += AggFunc.Frequency
        initialTopN = topN * 100 // Apply multiplier for base aggregation

      case func =>
        throw new UnsupportedOperationException(
          s"Unsupported aggregation function: $func for field $field"
        )
    }

    copy(
      aggWindows = SingleUnitWindow,
      windowUnit = windowUnit,
      topN = initialTopN,
      aggFuncs = initialAggFunctions.toSeq
    )
  }
}

object AggDefinition {
  private val YAML_BASE_PATH = "/jobconfigsv2"

  def loadConfig(dataSource: Option[String] = None, filePath: Option[String] = None): AggDefinition = {
    require(dataSource.nonEmpty || filePath.nonEmpty, "Both dataSource and filePath are empty")

    val configString = dataSource match {
      case Some(ds) => loadFromClassPath(ds)
      case None => loadByFilePath(filePath.get)
    }

    parseYamlConfig(configString)
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
          aggWindows = scalaAgg("aggWindows").asInstanceOf[java.util.List[Int]].asScala,
          windowUnit = scalaAgg("windowUnit").toString,
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