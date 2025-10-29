package com.thetradedesk.confetti

import com.amazonaws.services.s3.AmazonS3
import com.amazonaws.services.s3.model.S3Object
import com.thetradedesk.confetti.utils.Logger
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


// could be any case class.
case class CalibrationInputDataGeneratorJobConfig(
                                                   model: String,
                                                   tag: String,
                                                   version: Int,
                                                   lookBack: Int,
                                                   runDate: LocalDate,
                                                   startDate: LocalDate,
                                                   oosDataS3Bucket: String,
                                                   oosDataS3Path: String,
                                                   subFolderKey: String,
                                                   subFolderValue: String,
                                                   oosProdDataS3Path: String,
                                                   coalesceProdData: Boolean,
                                                   audienceResultCoalesce: Int,
                                                   outputPath: String,
                                                   outputCBPath: String
                                                 )

class ManualConfigLoaderSpec
    extends AnyFunSuite
    with Matchers
    with BeforeAndAfterAll
    with MockitoSugar {

  private val templatesPrefix = "configdata/confetti/config-templates/"
  private val templatesRoot = Paths.get(
    Option(getClass.getClassLoader.getResource(templatesPrefix.dropRight(1)))
      .getOrElse(throw new IllegalStateException("Missing test templates"))
      .toURI
  )
  private val simpleLogger: Logger = new Logger {
    override def debug(message: String): Unit = println(s"[ManualConfigLoader][DEBUG] $message")
    override def info(message: String): Unit = println(s"[ManualConfigLoader][INFO] $message")
    override def warn(message: String): Unit = Console.err.println(s"[ManualConfigLoader][WARN] $message")
    override def error(message: String): Unit = Console.err.println(s"[ManualConfigLoader][ERROR] $message")
  }

  private val currentVersion = "3842404-95a55169"
  private val currentVersionPath =
    "s3://thetradedesk-mlplatform-us-east-1/libs/audience/jars/prod/_CURRENT"
  private val s3Contents = Map(
    currentVersionPath -> s"$currentVersion\n"
  )

  private var originalS3Client: AnyRef = _

  override protected def beforeAll(): Unit = {
    super.beforeAll()
    originalS3Client = stubS3Client()
  }

  override protected def afterAll(): Unit = {
    restoreS3Client(originalS3Client)
    super.afterAll()
  }

  test("CalibrationInputDataGeneratorJob templates render into job config case class") {
    val expectedRunDate = LocalDate.of(2025, 10, 27)
    val expectedStartDate = LocalDate.of(2025, 3, 20)
    val expectedNamespace = "test/yanan-demo"
    val expectedVersionSuffix = expectedRunDate.format(DateTimeFormatter.BASIC_ISO_DATE) + "000000"
    val expectedModel = "RSMV2"
    val expectedTag = "Seed_None"

    val expectedConfig = CalibrationInputDataGeneratorJobConfig(
      model = expectedModel,
      tag = expectedTag,
      version = 2,
      lookBack = 8,
      runDate = expectedRunDate,
      startDate = expectedStartDate,
      oosDataS3Bucket = "thetradedesk-mlplatform-us-east-1",
      oosDataS3Path = s"data/prod/audience/$expectedModel/$expectedTag/v=2",
      subFolderKey = "mixedForward",
      subFolderValue = "Calibration",
      oosProdDataS3Path = s"data/prod/audience/$expectedModel/$expectedTag/v=1",
      coalesceProdData = false,
      audienceResultCoalesce = 4096,
      outputPath =
        s"s3://thetradedesk-mlplatform-us-east-1/data/$expectedNamespace/audience/$expectedModel/$expectedTag/v=1/$expectedVersionSuffix/mixedForward=Calibration",
      outputCBPath =
        s"s3://thetradedesk-mlplatform-us-east-1/data/$expectedNamespace/audience/$expectedModel/$expectedTag/v=2/$expectedVersionSuffix/mixedForward=Calibration"
    )

    // stands for manual provided overrides.
    val manualOverrides = stubConfig(
      Map(
        "version" -> expectedConfig.version.toString,
        "lookBack" -> expectedConfig.lookBack.toString,
        "runDate" -> expectedConfig.runDate.toString,
        "startDate" -> expectedConfig.startDate.toString,
        "oosDataS3Path" -> expectedConfig.oosDataS3Path,
        "audienceJarBranch" -> "master",
        "audienceJarVersion" -> "latest"
      )
    )

    try {
      val loader = new ManualConfigLoader[CalibrationInputDataGeneratorJobConfig](
        env = "test",
        experimentName = Some("yanan-demo"),
        groupName = "audience",
        jobName = "CalibrationInputDataGeneratorJob",
        simpleLogger
      )

      val result = loader.loadRuntimeConfigs()

      result.config shouldBe expectedConfig
      result.identityHash shouldBe "YHqpfY8pLrWimo0CxctJy7O2jIOdQSi0ibuL_EKb1Ek="
    } finally {
      restoreConfig(manualOverrides)
    }
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
