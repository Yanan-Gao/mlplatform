package com.thetradedesk.confetti

import com.thetradedesk.confetti.utils.{HashUtils, S3Utils}
import com.thetradedesk.spark.util.TTDConfig.config
import org.yaml.snakeyaml.{DumperOptions, Yaml}

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

    val yamlParser = new Yaml()
    val renderedTemplates = Map(
      IdentityTemplate -> "identity_config.yml",
      OutputTemplate -> "output_config.yml",
      ExecutionTemplate -> "execution_config.yml"
    ).map { case (templateName, outputName) =>
      val templateContent = readTemplate(templateName)
      val rendered = renderTemplate(templateContent, context)
      outputName -> (templateName -> rendered)
    }

    val processedIdentity = {
      val (templateName, identityContent) = renderedTemplates("identity_config.yml")
      val injectionResult = injectAudienceJarPath(identityContent, yamlParser)
      checkForUnresolvedVariables(injectionResult.renderedContent, templateName)
      injectionResult
    }

    val finalizedTemplates = renderedTemplates.map {
      case (outputName, (templateName, content)) if outputName == "identity_config.yml" =>
        outputName -> processedIdentity.renderedContent
      case (outputName, (templateName, content)) =>
        checkForUnresolvedVariables(content, templateName)
        outputName -> content
    }

    val combinedConfig = finalizedTemplates.values
      .map(parseYaml(_, yamlParser))
      .foldLeft(Map.empty[String, String])(_ ++ _)

    val identityHash = hashCanonical(processedIdentity.hashInput)

    combinedConfig + ("identity_config_id" -> identityHash)
  }

  private def readTemplate(templateName: String): String = {
    val base = getJobTemplatePath()
    val normalizedBase = if (base.endsWith("/")) base.dropRight(1) else base
    val path = s"$normalizedBase/$templateName"
    S3Utils.readFromS3(path)
  }

  private def renderTemplate(template: String, context: TemplateContext): String = {
    val placeholderRegex = "\\{\\{\\s*([^}]+)\\s*\\}\\}".r
    placeholderRegex.replaceAllIn(template, m => {
      val expression = m.group(1).trim
      evaluateExpression(expression, context).getOrElse(m.matched)
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

  private def hashCanonical(canonical: String): String = HashUtils.sha256Base64(canonical)

  private def buildTemplateContext(runtimeVars: Map[String, String]): TemplateContext = {
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
    val audienceJarContext =
      Seq(
        audienceJarBranch.map("audienceJarBranch" -> _),
        audienceJarVersion.map("audienceJarVersion" -> _)
      ).flatten.toMap

    val sanitizedRuntimeVars = runtimeVars - "run_date"

    TemplateContext(baseContext ++ sanitizedRuntimeVars ++ audienceJarContext, runDate)
  }

  private def determineRunDate(runtimeOverride: Option[String]): LocalDate = {
    val configDate = config.getStringOption("runDate")
      .orElse(config.getStringOption("confetti.runDate"))
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
    options.setSortKeys(true)
    options.setExplicitStart(false)
    options.setExplicitEnd(false)
    options.setLineBreak(DumperOptions.LineBreak.UNIX)
    options.setIndent(2)
    options.setIndicatorIndent(2)
    options.setWidth(4096)
    val yaml = new Yaml(options)
    yaml.dump(value)
  }

  private def evaluateExpression(expression: String, context: TemplateContext): Option[String] = {
    expression match {
      case strftime if strftime.startsWith("run_date.strftime") =>
        extractStrftimePattern(strftime, context).map(formatRunDate(_, context.runDate))
      case other => context.values.get(other)
    }
  }

  private def extractStrftimePattern(expression: String, context: TemplateContext): Option[String] = {
    val patternRegex = "run_date\\.strftime\\((.*)\\)".r
    expression match {
      case patternRegex(arg) =>
        val trimmed = arg.trim
        if ((trimmed.startsWith("\"") && trimmed.endsWith("\"")) || (trimmed.startsWith("'") && trimmed.endsWith("'")))) {
          Some(trimmed.substring(1, trimmed.length - 1))
        } else {
          context.values.get(trimmed)
        }
      case _ => None
    }
  }

  private def formatRunDate(pattern: String, runDate: LocalDate): String = {
    val sb = new StringBuilder
    var idx = 0
    while (idx < pattern.length) {
      val ch = pattern.charAt(idx)
      if (ch == '%') {
        if (idx + 1 >= pattern.length) {
          throw new IllegalArgumentException("Trailing '%' in strftime pattern")
        }
        val code = pattern.charAt(idx + 1)
        val replacement = code match {
          case 'Y' => f"${runDate.getYear}%04d"
          case 'y' => f"${runDate.getYear % 100}%02d"
          case 'm' => f"${runDate.getMonthValue}%02d"
          case 'd' => f"${runDate.getDayOfMonth}%02d"
          case 'H' => "00"
          case 'M' => "00"
          case 'S' => "00"
          case '%' => "%"
          case other => throw new IllegalArgumentException(s"Unsupported strftime code: %$other")
        }
        sb.append(replacement)
        idx += 2
      } else {
        sb.append(ch)
        idx += 1
      }
    }
    sb.toString()
  }

  private case class AudienceConfig(renderedContent: String, data: java.util.Map[String, Any]) {
    lazy val hashInput: String = canonicalizeForHash(data)
  }

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

  private case class TemplateContext(values: Map[String, String], runDate: LocalDate)

  private def getJobTemplatePath(): String = {
    s"s3://thetradedesk-mlplatform-us-east-1/configdata/confetti/config-templates/$groupName/$jobName"
  }
}
