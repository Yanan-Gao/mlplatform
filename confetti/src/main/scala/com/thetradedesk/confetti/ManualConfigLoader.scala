package com.thetradedesk.confetti

import com.hubspot.jinjava.{Jinjava, JinjavaConfig}
import com.hubspot.jinjava.objects.date.PyishDate
import com.thetradedesk.confetti.utils.{HashUtils, MapConfigReader, S3Utils}
import com.thetradedesk.confetti.utils.Logger
import com.thetradedesk.spark.util.TTDConfig.config
import org.yaml.snakeyaml.{DumperOptions, Yaml}

import java.time.format.DateTimeFormatter
import java.time.{LocalDate, ZoneOffset}
import scala.collection.JavaConverters._
import scala.reflect.ClassTag
import scala.reflect.runtime.universe._
import scala.util.Try
import com.fasterxml.jackson.dataformat.yaml.{YAMLFactory, YAMLMapper}
import com.fasterxml.jackson.module.scala.DefaultScalaModule
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule
import com.fasterxml.jackson.databind.SerializationFeature
import com.fasterxml.jackson.dataformat.yaml.YAMLGenerator

class ManualConfigLoader[C: TypeTag : ClassTag](env: String,
                                                experimentName: Option[String],
                                                groupName: String,
                                                jobName: String,
                                                logger: Logger
                                               ) {

  private val jinjava = new Jinjava(JinjavaConfig.newBuilder().withFailOnUnknownTokens(true).build())

  private val IdentityTemplate = "identity_config.yml.j2"
  private val OutputTemplate = "output_config.yml.j2"
  private val ExecutionTemplate = "execution_config.yml.j2"

  /**
   * Load the runtime configuration by rendering the job root templates with the
   * runtime variables and config derived values. Returns the flattened key/value
   * map used by downstream config readers with an additional identity hash.
   */
  def loadRuntimeConfigs(): ManualConfigLoader.RuntimeConfig[C] = {
    logger.info("Starting on manual resolving mode:")

    val resolvedFieldValues = resolveFieldValues()
    val runtimeContextVariables = buildRuntimeVariables(resolvedFieldValues)
    val context = buildTemplateContext(runtimeContextVariables)

    val templateSpecs = Seq(
      TemplateSpec(IdentityTemplate, "identity_config.yml", requiresIdentityProcessing = true),
      TemplateSpec(OutputTemplate, "output_config.yml"),
      TemplateSpec(ExecutionTemplate, "execution_config.yml")
    )

    val yamlParser = new Yaml()
    val renderedTemplates = renderTemplates(templateSpecs, context)
    val finalizedTemplates = finalizeRenderedTemplates(renderedTemplates, yamlParser)

    val combinedConfig = mergeRenderedConfigs(finalizedTemplates, yamlParser)
    val identityHash = hashCanonical(finalizedTemplates.identity.hashInput)

    val mergedConfig = (combinedConfig ++ runtimeContextVariables) + ("identity_config_id" -> identityHash)
    logger.info("Job identityHash: " + identityHash)
    logger.info("Final merged config : " + new Yaml().dump(mergedConfig.asJava))
    val configInstance = new MapConfigReader(mergedConfig, logger).as[C]
    logger.info("Final resolved config:\n" + YamlUtils.toYaml(configInstance))

    ManualConfigLoader.RuntimeConfig(configInstance, identityHash)
  }

  object YamlUtils {
    private val mapper = YAMLMapper.builder(new YAMLFactory()
        .disable(YAMLGenerator.Feature.WRITE_DOC_START_MARKER)
      )
      .addModule(DefaultScalaModule)
      .addModule(new JavaTimeModule)
      .disable(SerializationFeature.WRITE_DATES_AS_TIMESTAMPS)
      .build()

    def toYaml(value: Any): String =
      mapper.writerWithDefaultPrettyPrinter().writeValueAsString(value)
  }


  private def renderTemplates(specs: Seq[TemplateSpec], context: TemplateContext): Seq[RenderedTemplate] = {
    specs.map { spec =>
      val templateContent = readTemplate(spec.templateName)
      val rendered = renderTemplate(templateContent, context)
      RenderedTemplate(spec, rendered)
    }
  }

  private def finalizeRenderedTemplates(renderedTemplates: Seq[RenderedTemplate], yaml: Yaml): FinalizedTemplates = {
    val (identityTemplates, otherTemplates) = renderedTemplates.partition(_.spec.requiresIdentityProcessing)
    val identityTemplate = identityTemplates.headOption.getOrElse(
      throw new IllegalStateException("identity_config template is required but was not rendered")
    )

    val processedIdentity = {
      val injectionResult = injectAudienceJarPath(identityTemplate.renderedContent, yaml)
      checkForUnresolvedVariables(injectionResult.renderedContent, identityTemplate.spec.templateName)
      injectionResult
    }

    val finalizedOthers = otherTemplates.map { template =>
      checkForUnresolvedVariables(template.renderedContent, template.spec.templateName)
      template.spec.outputName -> template.renderedContent
    }.toMap

    val finalizedOutputs = finalizedOthers + (identityTemplate.spec.outputName -> processedIdentity.renderedContent)

    FinalizedTemplates(identityTemplate.spec, processedIdentity, finalizedOutputs)
  }

  private def mergeRenderedConfigs(finalizedTemplates: FinalizedTemplates, yaml: Yaml): Map[String, String] = {
    val identityOutputName = finalizedTemplates.identitySpec.outputName
    val identityMap = mapValuesToStrings(finalizedTemplates.identity.data)

    finalizedTemplates.outputs.foldLeft(identityMap) {
      case (acc, (outputName, content)) if outputName == identityOutputName => acc
      case (acc, (_, content)) => acc ++ parseYaml(content, yaml)
    }
  }

  private def mapValuesToStrings(data: java.util.Map[String, Any]): Map[String, String] = {
    data.asScala.map { case (k, v) => k.toString -> Option(v).map(_.toString).getOrElse("") }.toMap
  }

  private def readTemplate(templateName: String): String = {
    val base = getJobTemplatePath()
    val normalizedBase = if (base.endsWith("/")) base.dropRight(1) else base
    val path = s"$normalizedBase/$templateName"
    S3Utils.readFromS3(path)
  }

  private def renderTemplate(template: String, context: TemplateContext): String = {
    val result = jinjava.renderForResult(template, context.values.asJava)
    val errors = result.getErrors.asScala
    if (errors.nonEmpty) {
      val details = errors.map(_.toString).mkString("; ")
      throw new IllegalArgumentException(s"Failed to render template: $details")
    }
    result.getOutput
  }

  private def parseYaml(content: String, yaml: Yaml): Map[String, String] = {
    if (content.trim.isEmpty) Map.empty
    else {
      Option(yaml.load[Any](content)) match {
        case Some(map: java.util.Map[_, _]) =>
          map.asScala.map { case (k, v) => k.toString -> Option(v).map(_.toString).getOrElse("") }.toMap
        case Some(other) =>
          throw new IllegalArgumentException(s"Expected YAML map but found ${other.getClass.getSimpleName}")
        case None => Map.empty
      }
    }
  }

  private def checkForUnresolvedVariables(content: String, templateName: String): Unit = {
    val unresolvedPattern = "\\{\\{[^}]+\\}\\}".r
    val unresolved = unresolvedPattern.findAllMatchIn(content).map(_.matched).toSeq.distinct
    if (unresolved.nonEmpty) {
      val details = unresolved.mkString(", ")
      throw new IllegalArgumentException(
        s"Template $templateName contains unresolved variables: $details. " +
          s"This means that during this job runtime, the variables didn't get injected correctly."
      )
    }
  }

  private def hashCanonical(canonical: String): String = HashUtils.sha256Base64(canonical)

  private def buildTemplateContext(runtimeVars: Map[String, String]): TemplateContext = {
    val runDate = determineRunDate(runtimeVars.get("run_date").orElse(runtimeVars.get("runDate")))
    val runDateValue = new PyishDate(runDate.atStartOfDay(ZoneOffset.UTC)).withDateFormat("yyyy-MM-dd")

    val baseContextEntries: Map[String, AnyRef] = Map(
      "environment" -> env,
      "data_namespace" -> experimentName.map(exp => s"$env/$exp").getOrElse(env),
      "run_date" -> runDateValue,
      "run_date_format" -> "%Y-%m-%d",
      "version_date_format" -> "%Y%m%d",
      "full_version_date_format" -> "%Y%m%d000000"
    )

    val audienceJarBranch = config.getStringOption("audienceJarBranch").orElse(runtimeVars.get("audienceJarBranch"))
    val audienceJarVersion = config.getStringOption("audienceJarVersion").orElse(runtimeVars.get("audienceJarVersion"))
    val audienceJarContext =
      Seq(
        audienceJarBranch.map("audienceJarBranch" -> _),
        audienceJarVersion.map("audienceJarVersion" -> _)
      ).flatten.map { case (k, v) => k -> v.asInstanceOf[AnyRef] }.toMap

    val sanitizedRuntimeVars = runtimeVars - "run_date"

    val runtimeContext = sanitizedRuntimeVars.map { case (k, v) => k -> v.asInstanceOf[AnyRef] }.toMap

    TemplateContext(baseContextEntries ++ runtimeContext ++ audienceJarContext)
  }

  private def determineRunDate(runtimeOverride: Option[String]): LocalDate = {
    val configDate = config.getStringOption("date")
      .orElse(config.getStringOption("runDate"))
      .map(value => parseRequiredDate(value, "confetti runDate configuration"))

    val runtimeDate = runtimeOverride.map(value => parseRequiredDate(value, "runtime run_date"))

    configDate.orElse(runtimeDate).getOrElse(LocalDate.now(ZoneOffset.UTC))
  }

  private def parseDate(value: String): Option[LocalDate] = {
    val trimmed = value.trim
    Try(LocalDate.parse(trimmed)).orElse(Try(LocalDate.parse(trimmed, DateTimeFormatter.BASIC_ISO_DATE))).toOption
  }

  private def parseRequiredDate(value: String, source: String): LocalDate = {
    parseDate(value).getOrElse(
      throw new IllegalArgumentException(s"Invalid date '$value' supplied for $source. Expected yyyy-MM-dd or yyyyMMdd")
    )
  }

  private def resolveFieldValues(): Map[String, Any] = {
    val mirror = runtimeMirror(getClass.getClassLoader)
    val accessors = typeOf[C].members.collect {
      case m: MethodSymbol if m.isCaseAccessor => m
    }

    accessors.flatMap { accessor =>
      val fieldName = accessor.name.toString
      val fieldType = accessor.returnType
      resolveFieldValue(fieldName, fieldType)
    }.toMap
  }

  private def resolveFieldValue(fieldName: String, fieldType: Type): Option[(String, Any)] = {
    if (fieldType <:< typeOf[Option[_]]) {
      val innerType = fieldType.typeArgs.head
      val overrideValue = fetchOverride(fieldName, innerType)
      overrideValue.map(fieldName -> _)
    } else {
      val overrideValue = fetchOverride(fieldName, fieldType)
      overrideValue.map(fieldName -> _)
    }
  }

  private def fetchOverride(fieldName: String, fieldType: Type): Option[Any] = {
    parseOverrideValue(fieldName, fieldType, fieldName)
  }

  private def parseOverrideValue(configKey: String, fieldType: Type, fieldName: String): Option[Any] = {
    config.getStringOption(configKey).map { rawValue =>
      convertValue(rawValue.trim, fieldType, fieldName)
    }
  }

  private def convertValue(raw: String, fieldType: Type, fieldName: String): Any = {
    fieldType match {
      case t if t =:= typeOf[String] => raw
      case t if t =:= typeOf[Int] =>
        Try(raw.toInt).getOrElse(throw new IllegalArgumentException(s"Invalid Int override for $fieldName: '$raw'"))
      case t if t =:= typeOf[Long] =>
        Try(raw.toLong).getOrElse(throw new IllegalArgumentException(s"Invalid Long override for $fieldName: '$raw'"))
      case t if t =:= typeOf[Double] =>
        Try(raw.toDouble).getOrElse(throw new IllegalArgumentException(s"Invalid Double override for $fieldName: '$raw'"))
      case t if t =:= typeOf[Float] =>
        Try(raw.toFloat).getOrElse(throw new IllegalArgumentException(s"Invalid Float override for $fieldName: '$raw'"))
      case t if t =:= typeOf[Boolean] => parseBoolean(raw, fieldName)
      case t if t =:= typeOf[LocalDate] =>
        parseRequiredDate(raw, s"override for $fieldName")
      case t if t =:= typeOf[Seq[String]] => parseSeq(raw, identity[String])
      case t if t =:= typeOf[Seq[Int]] => parseSeq(raw, (v: String) =>
        Try(v.toInt).getOrElse(throw new IllegalArgumentException(s"Invalid Int value '$v' for $fieldName")))
      case t if t =:= typeOf[Seq[Long]] => parseSeq(raw, (v: String) =>
        Try(v.toLong).getOrElse(throw new IllegalArgumentException(s"Invalid Long value '$v' for $fieldName")))
      case t if t =:= typeOf[Seq[Double]] => parseSeq(raw, (v: String) =>
        Try(v.toDouble).getOrElse(throw new IllegalArgumentException(s"Invalid Double value '$v' for $fieldName")))
      case t if t =:= typeOf[Seq[Float]] => parseSeq(raw, (v: String) =>
        Try(v.toFloat).getOrElse(throw new IllegalArgumentException(s"Invalid Float value '$v' for $fieldName")))
      case t if t =:= typeOf[Seq[Boolean]] => parseSeq(raw, (v: String) => parseBoolean(v, fieldName))
      case t if t =:= typeOf[Seq[LocalDate]] => parseSeq(raw, (v: String) =>
        parseDate(v).getOrElse(throw new IllegalArgumentException(s"Invalid date '$v' for $fieldName")))
      case other =>
        throw new IllegalArgumentException(s"Unsupported override type $other for field $fieldName")
    }
  }

  private def parseSeq[T](raw: String, parse: String => T): Seq[T] = {
    if (raw.isEmpty) Seq.empty
    else raw.split(",").toSeq.map(_.trim).filter(_.nonEmpty).map(parse)
  }

  private def parseBoolean(raw: String, fieldName: String): Boolean = {
    raw.toLowerCase match {
      case "true" => true
      case "false" => false
      case other => throw new IllegalArgumentException(s"Invalid Boolean override for $fieldName: '$other'")
    }
  }

  private def buildRuntimeVariables(resolvedFieldValues: Map[String, Any]): Map[String, String] = {
    resolvedFieldValues.collect {
      case (name, value) if shouldIncludeRuntimeVariable(value) =>
        name -> valueToString(value)
    }
  }

  private def shouldIncludeRuntimeVariable(value: Any): Boolean = {
    value match {
      case null => false
      case s: String => s.trim.nonEmpty
      case opt: Option[_] => opt.exists(shouldIncludeRuntimeVariable)
      case _ => true
    }
  }

  private def valueToString(value: Any): String = {
    value match {
      case null => ""
      case d: LocalDate => d.toString
      case iterable: Iterable[_] => iterable.mkString(",")
      case other => other.toString
    }
  }

  private def injectAudienceJarPath(content: String, yaml: Yaml): AudienceConfig = {
    val parsed = Option(yaml.load[Any](content)).getOrElse(new java.util.LinkedHashMap[String, Any]())
    val data = parsed match {
      case map: java.util.Map[_, _] =>
        new java.util.LinkedHashMap[String, Any](map.asInstanceOf[java.util.Map[String, Any]])
      case other =>
        throw new IllegalArgumentException(s"identity_config.yml must be a YAML mapping but found ${other.getClass.getSimpleName}")
    }

    val branch = Option(data.get("audienceJarBranch")).map(_.toString)
      .getOrElse(throw new IllegalArgumentException("audienceJarBranch is required in Confetti config"))
    val version = Option(data.get("audienceJarVersion")).map(_.toString)
      .getOrElse(throw new IllegalArgumentException("audienceJarVersion is required in Confetti config"))

    val resolvedVersion = resolveAudienceJarVersion(branch, version)
    val jarPath = buildAudienceJarPath(branch, resolvedVersion)

    data.remove("audienceJarBranch")
    data.remove("audienceJarVersion")
    data.put("audienceJarPath", jarPath)

    val renderedContent = dumpSortedYaml(data)
    AudienceConfig(renderedContent, data)
  }

  private def resolveAudienceJarVersion(branch: String, version: String): String = {
    if (!version.equalsIgnoreCase("latest")) {
      version
    } else {
      val currentKey =
        if (branch == "master")
          "s3://thetradedesk-mlplatform-us-east-1/libs/audience/jars/prod/_CURRENT"
        else
          s"s3://thetradedesk-mlplatform-us-east-1/libs/audience/jars/mergerequests/$branch/_CURRENT"

      val content = S3Utils.readFromS3(currentKey)
      content.split("\n").find(_.trim.nonEmpty).map(_.trim)
        .getOrElse(throw new IllegalStateException(s"No version found in $currentKey"))
    }
  }

  private def buildAudienceJarPath(branch: String, version: String): String = {
    if (branch == "master") {
      s"s3://thetradedesk-mlplatform-us-east-1/libs/audience/jars/snapshots/master/$version/audience.jar"
    } else {
      s"s3://thetradedesk-mlplatform-us-east-1/libs/audience/jars/mergerequests/$branch/$version/audience.jar"
    }
  }

  private def dumpSortedYaml(value: java.util.Map[String, Any]): String = {
    val options = new DumperOptions()
    options.setDefaultFlowStyle(DumperOptions.FlowStyle.BLOCK)
    options.setPrettyFlow(false)
    options.setExplicitStart(false)
    options.setExplicitEnd(false)
    options.setLineBreak(DumperOptions.LineBreak.UNIX)
    options.setIndent(2)
    options.setIndicatorIndent(1)
    options.setWidth(4096)
    val yaml = new Yaml(options)
    val sorted = deepSortMaps(value).asInstanceOf[java.util.Map[String, Any]]
    yaml.dump(sorted)
  }

  private def deepSortMaps(value: Any): Any = {
    value match {
      case map: java.util.Map[_, _] =>
        val sortedEntries = map.asScala.toSeq.sortBy { case (k, _) => Option(k).map(_.toString).getOrElse("") }
        val linked = new java.util.LinkedHashMap[String, Any]()
        sortedEntries.foreach { case (k, v) =>
          val key = Option(k).map(_.toString).orNull
          linked.put(key, deepSortMaps(v))
        }
        linked
      case list: java.util.List[_] =>
        val collected = new java.util.ArrayList[Any](list.size())
        list.asScala.foreach(elem => collected.add(deepSortMaps(elem)))
        collected
      case other => other
    }
  }

  private case class FinalizedTemplates(identitySpec: TemplateSpec, identity: AudienceConfig, outputs: Map[String, String])

  private case class AudienceConfig(renderedContent: String, data: java.util.Map[String, Any]) {
    lazy val hashInput: String = canonicalizeForHash(data)
  }

  private case class RenderedTemplate(spec: TemplateSpec, renderedContent: String)

  private case class TemplateSpec(templateName: String, outputName: String, requiresIdentityProcessing: Boolean = false)

  private def canonicalizeForHash(value: Any): String = {
    flattenYaml(value).sortBy(_._1).map { case (k, v) => s"$k=$v" }.mkString("\n")
  }

  private def flattenYaml(value: Any, prefix: String = ""): Seq[(String, String)] = {
    value match {
      case map: java.util.Map[_, _] =>
        map.asScala.toSeq.flatMap { case (k, v) =>
          val key = if (prefix.isEmpty) k.toString else s"$prefix.${k.toString}"
          flattenYaml(v, key)
        }
      case list: java.util.List[_] =>
        list.asScala.zipWithIndex.flatMap { case (elem, idx) =>
          val key = if (prefix.isEmpty) s"[$idx]" else s"$prefix[$idx]"
          flattenYaml(elem, key)
        }
      case null => Seq(prefix -> "")
      case other => Seq(prefix -> other.toString)
    }
  }

  private case class TemplateContext(values: Map[String, AnyRef])

  private def getJobTemplatePath(): String = {
    s"s3://thetradedesk-mlplatform-us-east-1/configdata/confetti/config-templates/$groupName/$jobName"
  }
}

object ManualConfigLoader {
  case class RuntimeConfig[C](config: C, identityHash: String)
}
