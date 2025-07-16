package com.thetradedesk.featurestore.configs

import com.thetradedesk.featurestore.aggfunctions.AggFunctions.AggFuncV2
import com.thetradedesk.featurestore.constants.FeatureConstants.SingleUnitWindow
import com.thetradedesk.featurestore.rsm.CommonEnums.DataIntegrity.DataIntegrity
import com.thetradedesk.featurestore.rsm.CommonEnums.Grain.Grain
import com.thetradedesk.featurestore.rsm.CommonEnums.{DataIntegrity, Grain}
import com.thetradedesk.featurestore.ttdEnv
import org.yaml.snakeyaml.Yaml

import scala.collection.JavaConverters._
import scala.io.Source
import scala.util.{Failure, Success, Try}

case class DataSourceConfig(
                             name: String,
                             rootPath: String,
                             prefix: String,
                             format: String = "parquet",
                             loadQuery: Option[String] = None
                           )

case class AggTaskConfig(
                          outputRootPath: String,
                          outputPrefix: String,
                          windowGrain: Option[Grain] = None, // only applied for rollup config
                          dataIntegrity: DataIntegrity = DataIntegrity.AllExist
                        )

case class AggLevelConfig(
                           level: String,
                           saltSize: Int,
                           initAggGrains: Array[Grain],
                           initWritePartitions: Option[Int] = None,
                           rollupWritePartitions: Option[Int] = None,
                           enableFeatureKeyCount: Boolean = true
                         ) {
  override def equals(obj: Any): Boolean = obj match {
    case that: AggLevelConfig => this.level == that.level
    case _ => false
  }

  override def hashCode(): Int = level.hashCode
}

case class FieldAggSpec(
                         field: String,
                         dataType: String,
                         arraySize: Int = -1,
                         aggWindows: Seq[Int],
                         cardinality: Option[Int] = None,
                         aggFuncs: Seq[AggFuncV2]
                       ) {

  /** Extracts initial aggregation specification for the given time unit
   *
   * @return FieldAggSpec with initial aggregation functions
   */
  def extractFieldAggSpec: FieldAggSpec = {

    val mergeableFunc = AggFuncV2.getMergeableFuncs(aggFuncs)

    copy(
      aggWindows = SingleUnitWindow,
      aggFuncs = mergeableFunc
    )
  }
}

case class AggDefinition(
                          dataSource: DataSourceConfig,
                          aggLevels: Set[AggLevelConfig],
                          initAggConfig: AggTaskConfig,
                          rollupAggConfig: AggTaskConfig,
                          aggregations: Seq[FieldAggSpec]
                        ) {

  /** Extracts initial aggregation definition for the given time grain */
  def extractInitialAggDefinition(): AggDefinition = {
    val initialAggregations = aggregations.map(_.extractFieldAggSpec)
    copy(aggregations = initialAggregations)
  }
}

object AggDefinition {
  private val YAML_BASE_PATH =
    if (ttdEnv == "local") "/jobconfigsv2_local" else "/jobconfigsv2"

  def loadConfig(
                  dataSource: Option[String] = None,
                  filePath: Option[String] = None
                ): AggDefinition = {
    require(
      dataSource.nonEmpty || filePath.nonEmpty,
      "Both dataSource and filePath are empty"
    )

    val configString = dataSource match {
      case Some(ds) => loadFromClassPath(ds)
      case None => loadByFilePath(filePath.get)
    }

    val config = parseYamlConfig(configString)
    validateConfig(config)
    config
  }

  private def validateConfig(config: AggDefinition): Unit = {

    val initAggDef = config.extractInitialAggDefinition()
    val allFields = initAggDef.aggregations.map(_.field).toSet
    val allAggFields = initAggDef.aggregations.flatMap(agg => agg.aggFuncs.map(func => (s"${agg.field}_${func}"))).toSet
    val conflicts = allFields intersect allAggFields
    if (conflicts.nonEmpty) {
      throw new IllegalArgumentException(
        s"Column name conflicts found: ${conflicts.mkString(", ")}"
      )
    }
  }

  private def parseYamlConfig(configString: String): AggDefinition = {
    val yaml = new Yaml()

    Try {
      val javaMap = yaml.load[java.util.Map[String, Any]](configString)
      val yamlMap = javaMap.asScala.toMap

      val aggregations = parseAggregations(yamlMap)
      val dataSourceMap = yamlMap("dataSource").asInstanceOf[java.util.Map[String, Any]].asScala.toMap
      val initAggConfigMap = yamlMap("initAggConfig").asInstanceOf[java.util.Map[String, Any]].asScala.toMap
      val rollupAggConfigMap = yamlMap("rollupAggConfig").asInstanceOf[java.util.Map[String, Any]].asScala.toMap

      AggDefinition(
        dataSource = DataSourceConfig(
          name = dataSourceMap("name").toString,
          rootPath = dataSourceMap("rootPath").toString,
          prefix = dataSourceMap("prefix").toString,
          format = dataSourceMap.getOrElse("format", "parquet").toString,
          loadQuery = dataSourceMap.get("loadQuery").map(_.toString)
        ),
        aggLevels = parseAggLevels(yamlMap),
        initAggConfig = AggTaskConfig(
          outputRootPath = initAggConfigMap("outputRootPath").toString,
          outputPrefix = initAggConfigMap("outputPrefix").toString,
          dataIntegrity = DataIntegrity.fromString(initAggConfigMap.getOrElse("dataIntegrity", "all_exist").toString)
        ),
        rollupAggConfig = AggTaskConfig(
          outputRootPath = initAggConfigMap("outputRootPath").toString,
          outputPrefix = rollupAggConfigMap("outputPrefix").toString,
          windowGrain = Some(Grain.fromString(rollupAggConfigMap("windowGrain").toString).getOrElse(throw new IllegalArgumentException(s"Unable to parse window grain from " +
            s": ${rollupAggConfigMap("windowGrain")}"))),
          dataIntegrity = DataIntegrity.fromString(rollupAggConfigMap.getOrElse("dataIntegrity", "all_exist").toString)
        ),
        aggregations = aggregations
      )
    } match {
      case Success(config) => config
      case Failure(e) =>
        throw new IllegalArgumentException(
          s"Failed to parse YAML configuration: $configString",
          e
        )
    }
  }

  private def parseAggLevels(yamlMap: Map[String, Any]): Set[AggLevelConfig] = {
    yamlMap("aggLevels")
      .asInstanceOf[java.util.List[java.util.Map[String, Any]]]
      .asScala
      .map { agg =>
        val scalaAgg = agg.asScala.toMap
        AggLevelConfig(
          level = scalaAgg("level").toString,
          saltSize = scalaAgg.getOrElse("saltSize", -1).asInstanceOf[Int],
          initAggGrains = scalaAgg("initAggGrains")
            .asInstanceOf[java.util.List[String]]
            .asScala
            .map(str => Grain.fromString(str).getOrElse(throw new IllegalArgumentException(s"Unable to parse initial grain from : $str"))).toArray,
          initWritePartitions = scalaAgg.get("initWritePartitions").map(_.asInstanceOf[Int]),
          rollupWritePartitions = scalaAgg.get("rollupWritePartitions").map(_.asInstanceOf[Int]).orElse(scalaAgg.get("initWritePartitions").map(_.asInstanceOf[Int])),
          enableFeatureKeyCount = scalaAgg.getOrElse("enableFeatureKeyCount", true).asInstanceOf[Boolean]
        )
      }
      .toSet
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
          cardinality = scalaAgg.get("cardinality").map(_.asInstanceOf[Int]),
          aggFuncs = parseAggFunctions(scalaAgg, aggField)
        )
      }
  }

  private def parseAggFunctions(scalaAgg: Map[String, Any], aggField: String): Seq[AggFuncV2] = {
    val strList = scalaAgg("aggFunc")
      .asInstanceOf[java.util.List[String]]
      .asScala
    AggFuncV2.parseAggFuncs(strList, aggField)
  }

  private def loadByFilePath(filePath: String): String = {
    Try {
      val inputStream = new java.io.FileInputStream(filePath)
      if (inputStream == null) {
        throw new IllegalArgumentException(s"YAML file not found: $filePath")
      }
      Source.fromInputStream(inputStream).mkString
    }.getOrElse(
      throw new IllegalArgumentException(s"Failed to load YAML file: $filePath")
    )
  }

  private def loadFromClassPath(dataSource: String): String = {
    val yamlPath = s"$YAML_BASE_PATH/Agg${dataSource}.yml"
    Try {
      val inputStream = getClass.getResourceAsStream(yamlPath)
      if (inputStream == null) {
        throw new IllegalArgumentException(s"YAML file not found: $yamlPath")
      }
      Source.fromInputStream(inputStream).mkString
    }.getOrElse(
      throw new IllegalArgumentException(
        s"Failed to load YAML from classpath: $yamlPath"
      )
    )
  }
}
