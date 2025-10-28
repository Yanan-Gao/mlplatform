package com.thetradedesk.confetti

import com.amazonaws.services.s3.AmazonS3
import com.amazonaws.services.s3.model.S3Object
import com.thetradedesk.audience.jobs.CalibrationInputDataGeneratorJobConfig
import com.thetradedesk.confetti.utils.HashUtils
import com.typesafe.config.ConfigFactory
import org.mockito.ArgumentMatchers.anyString
import org.mockito.MockitoSugar
import org.mockito.invocation.InvocationOnMock
import org.scalatest.BeforeAndAfterAll
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers

import java.io.ByteArrayInputStream
import java.nio.charset.StandardCharsets
import java.nio.file.{Files, Paths}
import java.time.LocalDate
import java.time.format.DateTimeFormatter

class ManualConfigLoaderSpec
    extends AnyFunSuite
    with Matchers
    with BeforeAndAfterAll
    with MockitoSugar {

  case class ExampleConfig(
      manualParameter: Double,
      optionalFlag: Option[Boolean],
      run_date: LocalDate,
      audienceJarBranch: Option[String],
      audienceJarVersion: Option[String],
      audienceJarPath: Option[String],
      custom_field: Option[Double],
      output_threshold: Option[Double],
      execution_flag: Option[Boolean],
      identity_config_id: String
  )

  private val groupName = "test-group"
  private val jobName = "test-job"
  private val templateBase =
    s"s3://thetradedesk-mlplatform-us-east-1/configdata/confetti/config-templates/$groupName/$jobName"
  private val currentVersion = "20240115"

  private val templatesPrefix = "configdata/confetti/config-templates/"
  private val templatesRoot = Paths.get(
    Option(getClass.getClassLoader.getResource(templatesPrefix.dropRight(1)))
      .getOrElse(throw new IllegalStateException("Missing test templates"))
      .toURI
  )
  private val currentVersionPath =
    "s3://thetradedesk-mlplatform-us-east-1/libs/audience/jars/mergerequests/feature-branch/_CURRENT"
  private val s3Contents = Map(
    currentVersionPath -> s"$currentVersion\n"
  )

  private var originalS3Client: AnyRef = _
  private var originalConfig: AnyRef = _

  override protected def beforeAll(): Unit = {
    super.beforeAll()
    originalS3Client = stubS3Client()
    originalConfig = stubConfig(
      Map(
        "manualParameter" -> "2.5",
        "optionalFlag" -> "true",
        "audienceJarBranch" -> "feature-branch",
        "audienceJarVersion" -> "latest"
      )
    )
  }

  override protected def afterAll(): Unit = {
    restoreS3Client(originalS3Client)
    restoreConfig(originalConfig)
    super.afterAll()
  }

  test("loadRuntimeConfigs renders templates, applies overrides, and computes identity hash") {
    val loader = new ManualConfigLoader[ExampleConfig](
      env = "prod",
      experimentName = Some("experiment"),
      groupName = groupName,
      jobName = jobName
    )

    val baseConfig = ExampleConfig(
      manualParameter = 1.0,
      optionalFlag = None,
      run_date = LocalDate.of(2024, 5, 1),
      audienceJarBranch = None,
      audienceJarVersion = None,
      audienceJarPath = None,
      custom_field = None,
      output_threshold = None,
      execution_flag = None,
      identity_config_id = ""
    )

    val result = loader.loadRuntimeConfigs(baseConfig)

    val expectedJarPath =
      s"s3://thetradedesk-mlplatform-us-east-1/libs/audience/jars/mergerequests/feature-branch/$currentVersion/audience.jar"
    val canonical = Seq(
      s"audienceJarPath=$expectedJarPath",
      "custom_field=2.5"
    ).mkString("\n")
    val expectedHash = HashUtils.sha256Base64(canonical)

    result.manualParameter shouldBe 2.5
    result.optionalFlag shouldBe Some(true)
    result.run_date shouldBe LocalDate.of(2024, 5, 1)
    result.audienceJarBranch shouldBe Some("feature-branch")
    result.audienceJarVersion shouldBe Some("latest")
    result.audienceJarPath shouldBe Some(expectedJarPath)
    result.custom_field shouldBe Some(2.5)
    result.output_threshold shouldBe Some(2.5)
    result.execution_flag shouldBe Some(true)
    result.identity_config_id shouldBe expectedHash
  }

  test("CalibrationInputDataGeneratorJob templates render into job config case class") {
    val loader = new ManualConfigLoader[CalibrationInputDataGeneratorJobConfig](
      env = "prod",
      experimentName = Some("experiment"),
      groupName = "audience",
      jobName = "CalibrationInputDataGeneratorJob"
    )

    val baseConfig = CalibrationInputDataGeneratorJobConfig(
      model = "RSMV2",
      tag = "Seed_None",
      version = 2,
      lookBack = 10,
      runDate = LocalDate.of(2024, 4, 5),
      startDate = LocalDate.of(2024, 3, 1),
      oosDataS3Bucket = "",
      oosDataS3Path = "",
      subFolderKey = "",
      subFolderValue = "",
      oosProdDataS3Path = "",
      coalesceProdData = false,
      audienceResultCoalesce = 4096,
      outputPath = "",
      outputCBPath = ""
    )

    val result = loader.loadRuntimeConfigs(baseConfig)

    val expectedNamespace = "prod/experiment"
    val expectedVersionSuffix = baseConfig.runDate.format(DateTimeFormatter.BASIC_ISO_DATE) + "000000"
    val expectedOutputPath =
      s"s3://thetradedesk-mlplatform-us-east-1/data/$expectedNamespace/audience/${baseConfig.model}/${baseConfig.tag}/v=1/$expectedVersionSuffix/mixedForward=Calibration"
    val expectedOutputCBPath =
      s"s3://thetradedesk-mlplatform-us-east-1/data/$expectedNamespace/audience/${baseConfig.model}/${baseConfig.tag}/v=2/$expectedVersionSuffix/mixedForward=Calibration"

    result.model shouldBe baseConfig.model
    result.tag shouldBe baseConfig.tag
    result.version shouldBe baseConfig.version
    result.lookBack shouldBe baseConfig.lookBack
    result.runDate shouldBe baseConfig.runDate
    result.startDate shouldBe baseConfig.startDate
    result.oosDataS3Bucket shouldBe "thetradedesk-mlplatform-us-east-1"
    result.oosDataS3Path shouldBe s"data/$expectedNamespace/audience/${baseConfig.model}/${baseConfig.tag}/v=2"
    result.subFolderKey shouldBe "mixedForward"
    result.subFolderValue shouldBe "Calibration"
    result.oosProdDataS3Path shouldBe "data/prod/audience/RSMV2/Seed_None/v=1"
    result.coalesceProdData shouldBe false
    result.audienceResultCoalesce shouldBe baseConfig.audienceResultCoalesce
    result.outputPath shouldBe expectedOutputPath
    result.outputCBPath shouldBe expectedOutputCBPath
  }

  private def stubS3Client(): AnyRef = {
    val moduleClass = Class.forName("com.thetradedesk.confetti.utils.S3Utils$")
    val module = moduleClass.getField("MODULE$").get(null)
    val field = moduleClass.getDeclaredField("s3Client")
    field.setAccessible(true)
    val original = field.get(module)

    val s3Mock = mock[AmazonS3]
    when(s3Mock.getObject(anyString(), anyString())).thenAnswer { invocation: InvocationOnMock =>
      val bucket = invocation.getArgument[String](0)
      val key = invocation.getArgument[String](1)
      val path = s"s3://$bucket/$key"
      val content =
        if (key.startsWith(templatesPrefix)) {
          val relative = key.stripPrefix(templatesPrefix)
          val file = templatesRoot.resolve(relative)
          new String(Files.readAllBytes(file), StandardCharsets.UTF_8)
        } else {
          s3Contents.getOrElse(path, throw new IllegalArgumentException(s"Unexpected S3 path $path"))
        }
      val s3Object = new S3Object()
      val bytes = content.getBytes(StandardCharsets.UTF_8)
      s3Object.setObjectContent(new ByteArrayInputStream(bytes))
      s3Object
    }

    field.set(module, s3Mock)
    original
  }

  private def restoreS3Client(original: AnyRef): Unit = {
    val moduleClass = Class.forName("com.thetradedesk.confetti.utils.S3Utils$")
    val module = moduleClass.getField("MODULE$").get(null)
    val field = moduleClass.getDeclaredField("s3Client")
    field.setAccessible(true)
    field.set(module, original)
  }

  private def stubConfig(overrides: Map[String, String]): AnyRef = {
    val moduleClass = Class.forName("com.thetradedesk.spark.util.TTDConfig$")
    val module = moduleClass.getField("MODULE$").get(null)
    val getter = moduleClass.getMethod("config")
    val original = getter.invoke(module)

    val configClass = original.getClass
    val setter = moduleClass.getMethod("config_$eq", configClass)

    val ctor = configClass.getConstructors.collectFirst {
      case c if c.getParameterCount == 1 && classOf[com.typesafe.config.Config].isAssignableFrom(c.getParameterTypes.head) =>
        c
    }.getOrElse(throw new IllegalStateException("Unable to locate RichConfig constructor"))

    val configMap = new java.util.HashMap[String, AnyRef]()
    overrides.foreach { case (k, v) => configMap.put(k, v) }
    val typesafeConfig = ConfigFactory.parseMap(configMap)
    val newConfig = ctor.newInstance(typesafeConfig).asInstanceOf[AnyRef]

    setter.invoke(module, newConfig)
    original
  }

  private def restoreConfig(original: AnyRef): Unit = {
    val moduleClass = Class.forName("com.thetradedesk.spark.util.TTDConfig$")
    val module = moduleClass.getField("MODULE$").get(null)
    val setter = moduleClass.getMethod("config_$eq", original.getClass)
    setter.invoke(module, original)
  }
}
