package com.thetradedesk.confetti

import com.thetradedesk.confetti.utils.{HashUtils, S3Utils}
import com.thetradedesk.spark.util.TTDConfig.config
import org.yaml.snakeyaml.Yaml

import java.time.format.DateTimeFormatter
import java.time.{LocalDate, ZoneOffset}
import scala.collection.JavaConverters._
import scala.util.Try

class ManualConfigLoader(env: String, experimentName: Option[String], groupName: String, jobName: String) {

  private val IdentityTemplate = "identity_config.yml.j2"
  private val OutputTemplate = "output_config.yml.j2"
  private val ExecutionTemplate = "execution_config.yml.j2"

  /**
   * Load the runtime configuration by rendering the job root templates with the
   * runtime variables and config derived values. Returns the flattened key/value
   * map used by downstream config readers with an additional identity hash.
   */
  def loadRuntimeConfigs(runtimeVars: Map[String, String]): Map[String, String] = {
    val context = buildTemplateContext(runtimeVars)

    val renderedTemplates = Map(
      IdentityTemplate -> "identity_config.yml",
      OutputTemplate -> "output_config.yml",
      ExecutionTemplate -> "execution_config.yml"
    ).map { case (templateName, outputName) =>
      val templateContent = readTemplate(templateName)
      val renderedContent = renderTemplate(templateContent, context)
      checkForUnresolvedVariables(renderedContent, templateName)
      outputName -> renderedContent
    }

    val yamlParser = new Yaml()
    val combinedConfig = renderedTemplates.values
      .map(parseYaml(_, yamlParser))
      .foldLeft(Map.empty[String, String])(_ ++ _)

    val identityConfig = parseYaml(renderedTemplates("identity_config.yml"), yamlParser)
    val identityHash = hashSorted(identityConfig)

    combinedConfig + ("identity_config_id" -> identityHash)
  }

  private def readTemplate(templateName: String): String = {
    val base = getJobTemplatePath()
    val normalizedBase = if (base.endsWith("/")) base.dropRight(1) else base
    val path = s"$normalizedBase/$templateName"
    S3Utils.readFromS3(path)
  }

  private def renderTemplate(template: String, context: Map[String, String]): String = {
    val placeholderRegex = "\\{\\{\\s*([^}]+)\\s*\\}\\}".r
    placeholderRegex.replaceAllIn(template, m => {
      val key = m.group(1).trim
      context.getOrElse(key, m.matched)
    })
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

  private def hashSorted(values: Map[String, String]): String = {
    val canonical = values.toSeq.sortBy(_._1).map { case (k, v) => s"$k=$v" }.mkString("\n")
    HashUtils.sha256Base64(canonical)
  }

  private def buildTemplateContext(runtimeVars: Map[String, String]): Map[String, String] = {
    val environment = config.getStringOption("confettiEnv")
      .orElse(config.getStringOption("ttd.env"))
      .getOrElse(env)

    val configuredExperiment = config.getStringOption("experimentName").filter(_.nonEmpty)
    val effectiveExperiment = configuredExperiment.orElse(experimentName.filter(_.nonEmpty))

    val runDate = determineRunDate(runtimeVars.get("run_date"))

    val baseContext = Map(
      "environment" -> environment,
      "data_namespace" -> effectiveExperiment.map(exp => s"$environment/$exp").getOrElse(environment),
      "run_date" -> runDate.format(DateTimeFormatter.ISO_LOCAL_DATE),
      "run_date_format" -> "%Y-%m-%d",
      "version_date_format" -> "%Y%m%d",
      "full_version_date_format" -> "%Y%m%d000000"
    ) ++ effectiveExperiment.map(exp => "experimentName" -> exp)

    val audienceJarBranch = config.getStringOption("audienceJarBranch").orElse(runtimeVars.get("audienceJarBranch"))
    val audienceJarVersion = config.getStringOption("audienceJarVersion").orElse(runtimeVars.get("audienceJarVersion"))
    val audienceJarContext = resolveAudienceJarPath(audienceJarBranch, audienceJarVersion).map("audienceJarPath" -> _).toMap

    runtimeVars ++ baseContext ++ audienceJarContext
  }

  private def determineRunDate(runtimeOverride: Option[String]): LocalDate = {
    val configDate = config.getStringOption("runDate")
      .orElse(config.getStringOption("confetti.runDate"))
      .flatMap(parseDate)

    configDate
      .orElse(runtimeOverride.flatMap(parseDate))
      .getOrElse(LocalDate.now(ZoneOffset.UTC))
  }

  private def parseDate(value: String): Option[LocalDate] = Try(LocalDate.parse(value.trim)).toOption

  private def resolveAudienceJarPath(branch: Option[String], version: Option[String]): Option[String] = {
    // Placeholder implementation; real resolution logic will be provided later.
    None
  }

  private def getJobTemplatePath(): String = {
    s"s3://thetradedesk-mlplatform-us-east-1/configdata/confetti/config-templates/$groupName/$jobName"
  }
}
