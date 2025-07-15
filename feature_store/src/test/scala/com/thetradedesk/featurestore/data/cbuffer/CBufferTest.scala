package com.thetradedesk.featurestore.data.cbuffer

import com.github.mrpowers.spark.fast.tests.DatasetComparer
import com.thetradedesk.featurestore.configs.{DataType, FeatureDefinition, FeatureSourceDefinition, UserFeatureMergeDefinition}
import com.thetradedesk.featurestore.constants.FeatureConstants.UserIDKey
import com.thetradedesk.featurestore.data.cbuffer.SchemaHelper.{CBufferDataFrameReader, CBufferDataFrameWriter}
import com.thetradedesk.featurestore.data.metrics.UserFeatureMergeJobTelemetry
import com.thetradedesk.featurestore.testutils.{MockData, TTDSparkTest}
import com.thetradedesk.featurestore.utils.DatasetHelper.refineDataFrame
import com.thetradedesk.spark.util.io.FSUtils
import com.thetradedesk.spark.util.prometheus.PrometheusClient

import java.nio.file.Files

class CBufferTest extends TTDSparkTest with DatasetComparer {
  private implicit val prometheus = new PrometheusClient("FeaturesJobTest", "UserFeatureMergeJobTest")
  private implicit val telemetry = UserFeatureMergeJobTelemetry()

  private val tempFolder = Files.createTempDirectory("CustomBufferDataGeneratorTest").toUri.getPath
  private var tempFolderIndex = 0

  private def subTempFolder(): String = {
    tempFolderIndex += 1
    s"${tempFolder}${tempFolderIndex}"
  }

  private val userFeatureFolder: String = "user_features"
  private var globalInc: Int = 0

  def nextIndex() = {
    globalInc += 1
    globalInc
  }

  def testDataGenerationAndValidate(userFeatureMergeDefinition: UserFeatureMergeDefinition, enableBinary: Boolean = true) = {
    val schema = MockData.featureSourceSchema(userFeatureMergeDefinition, enableBinary, cbuffer = true)

    val data = Seq(
      MockData.randomFeatureSourceDataMock(userFeatureMergeDefinition, cbuffer = true),
      MockData.randomFeatureSourceDataMock(userFeatureMergeDefinition, cbuffer = true),
      MockData.randomFeatureSourceDataMock(userFeatureMergeDefinition, cbuffer = true),
      MockData.randomFeatureSourceDataMock(userFeatureMergeDefinition, cbuffer = true),
      MockData.randomFeatureSourceDataMock(userFeatureMergeDefinition, cbuffer = true)
    )
    val df = spark.createDataFrame(spark.sparkContext.parallelize(data), schema)
    df.printSchema()
    df.show(10, false)

    // validate cbuffer raw-based format
    val cbufferPath = subTempFolder()
    println("cbuffer file path: " + cbufferPath)
    df
      .write
      .option("maxChunkRecordCount", "2")
      .option("defaultChunkRecordSize", "2")
      .cb(cbufferPath)

    val df2 = spark
      .read
      .option("supportBinary", enableBinary.toString)
      .cb(cbufferPath)

    assertSmallDatasetEquality(refineDataFrame(df2, UserIDKey), refineDataFrame(df, UserIDKey))

    // validate cbuffer column-based format
    val cbufferPath2 = subTempFolder()
    println("cbuffer file path: " + cbufferPath)
    df
      .write
      .option("maxChunkRecordCount", "2")
      .option("defaultChunkRecordSize", "2")
      .option("columnBasedChunk", "true")
      .cb(cbufferPath2)

    val df3 = spark
      .read
      .option("supportBinary", enableBinary.toString)
      .cb(cbufferPath2)

    assertSmallDatasetEquality(refineDataFrame(df3, UserIDKey), refineDataFrame(df, UserIDKey))
  }

  test("run generate and validate result - boolean") {
    val userFeatureMergeDefinition = new UserFeatureMergeDefinition(
      "ufmd1", userFeatureFolder, Array(
        FeatureSourceDefinition(s"s${nextIndex()}", s"p${nextIndex()}", userFeatureFolder,
          Array(
            FeatureDefinition(s"f${nextIndex()}", DataType.Bool),
            FeatureDefinition(s"f${nextIndex()}", DataType.Bool),
          )),
        FeatureSourceDefinition(s"s${nextIndex()}", s"p${nextIndex()}", userFeatureFolder,
          Array(
            FeatureDefinition(s"f${nextIndex()}", DataType.Bool),
          )),
        FeatureSourceDefinition(s"s${nextIndex()}", s"p${nextIndex()}", userFeatureFolder,
          Array(
            FeatureDefinition(s"f${nextIndex()}", DataType.Bool),
            FeatureDefinition(s"f${nextIndex()}", DataType.Bool),
          )))
    )

    testDataGenerationAndValidate(userFeatureMergeDefinition)
  }

  test("run generate and validate result - byte") {
    val userFeatureMergeDefinition = new UserFeatureMergeDefinition(
      "ufmd1", userFeatureFolder, Array(
        FeatureSourceDefinition(s"s${nextIndex()}", s"p${nextIndex()}", userFeatureFolder,
          Array(
            FeatureDefinition(s"f${nextIndex()}", DataType.Byte),
            FeatureDefinition(s"f${nextIndex()}", DataType.Byte),
          )),
        FeatureSourceDefinition(s"s${nextIndex()}", s"p${nextIndex()}", userFeatureFolder,
          Array(
            FeatureDefinition(s"f${nextIndex()}", DataType.Byte),
          )),
        FeatureSourceDefinition(s"s${nextIndex()}", s"p${nextIndex()}", userFeatureFolder,
          Array(
            FeatureDefinition(s"f${nextIndex()}", DataType.Byte),
          )))
    )

    testDataGenerationAndValidate(userFeatureMergeDefinition)
    testDataGenerationAndValidate(userFeatureMergeDefinition, enableBinary = false)
  }

  test("run generate and validate result - short") {
    val userFeatureMergeDefinition = new UserFeatureMergeDefinition(
      "ufmd1", userFeatureFolder, Array(
        FeatureSourceDefinition(s"s${nextIndex()}", s"p${nextIndex()}", userFeatureFolder,
          Array(
            FeatureDefinition(s"f${nextIndex()}", DataType.Short),
            FeatureDefinition(s"f${nextIndex()}", DataType.Short),
          )),
        FeatureSourceDefinition(s"s${nextIndex()}", s"p${nextIndex()}", userFeatureFolder,
          Array(
            FeatureDefinition(s"f${nextIndex()}", DataType.Short),
            FeatureDefinition(s"f${nextIndex()}", DataType.Short),
          )),
        FeatureSourceDefinition(s"s${nextIndex()}", s"p${nextIndex()}", userFeatureFolder,
          Array(
            FeatureDefinition(s"f${nextIndex()}", DataType.Short),
          )))
    )

    testDataGenerationAndValidate(userFeatureMergeDefinition)
  }

  test("run generate and validate result - int") {
    val userFeatureMergeDefinition = new UserFeatureMergeDefinition(
      "ufmd1", userFeatureFolder, Array(
        FeatureSourceDefinition(s"s${nextIndex()}", s"p${nextIndex()}", userFeatureFolder,
          Array(
            FeatureDefinition(s"f${nextIndex()}", DataType.Int),
          )),
        FeatureSourceDefinition(s"s${nextIndex()}", s"p${nextIndex()}", userFeatureFolder,
          Array(
            FeatureDefinition(s"f${nextIndex()}", DataType.Int),
            FeatureDefinition(s"f${nextIndex()}", DataType.Int),
          )),
        FeatureSourceDefinition(s"s${nextIndex()}", s"p${nextIndex()}", userFeatureFolder,
          Array(
            FeatureDefinition(s"f${nextIndex()}", DataType.Int),
          )))
    )

    testDataGenerationAndValidate(userFeatureMergeDefinition)
  }

  test("run generate and validate result - long") {
    val userFeatureMergeDefinition = new UserFeatureMergeDefinition(
      "ufmd1", userFeatureFolder, Array(
        FeatureSourceDefinition(s"s${nextIndex()}", s"p${nextIndex()}", userFeatureFolder,
          Array(
            FeatureDefinition(s"f${nextIndex()}", DataType.Long),
            FeatureDefinition(s"f${nextIndex()}", DataType.Long),
          )),
        FeatureSourceDefinition(s"s${nextIndex()}", s"p${nextIndex()}", userFeatureFolder,
          Array(
            FeatureDefinition(s"f${nextIndex()}", DataType.Long),
            FeatureDefinition(s"f${nextIndex()}", DataType.Long),
          )),
        FeatureSourceDefinition(s"s${nextIndex()}", s"p${nextIndex()}", userFeatureFolder,
          Array(
            FeatureDefinition(s"f${nextIndex()}", DataType.Long),
            FeatureDefinition(s"f${nextIndex()}", DataType.Long),
          )))
    )

    testDataGenerationAndValidate(userFeatureMergeDefinition)
  }

  test("run generate and validate result - float") {
    val userFeatureMergeDefinition = new UserFeatureMergeDefinition(
      "ufmd1", userFeatureFolder, Array(
        FeatureSourceDefinition(s"s${nextIndex()}", s"p${nextIndex()}", userFeatureFolder,
          Array(
            FeatureDefinition(s"f${nextIndex()}", DataType.Float),
            FeatureDefinition(s"f${nextIndex()}", DataType.Float),
          )),
        FeatureSourceDefinition(s"s${nextIndex()}", s"p${nextIndex()}", userFeatureFolder,
          Array(
            FeatureDefinition(s"f${nextIndex()}", DataType.Float),
          )),
        FeatureSourceDefinition(s"s${nextIndex()}", s"p${nextIndex()}", userFeatureFolder,
          Array(
            FeatureDefinition(s"f${nextIndex()}", DataType.Float),
          )))
    )

    testDataGenerationAndValidate(userFeatureMergeDefinition)
  }

  test("run generate and validate result - double") {
    val userFeatureMergeDefinition = new UserFeatureMergeDefinition(
      "ufmd1", userFeatureFolder, Array(
        FeatureSourceDefinition(s"s${nextIndex()}", s"p${nextIndex()}", userFeatureFolder,
          Array(
            FeatureDefinition(s"f${nextIndex()}", DataType.Double),
          )),
        FeatureSourceDefinition(s"s${nextIndex()}", s"p${nextIndex()}", userFeatureFolder,
          Array(
            FeatureDefinition(s"f${nextIndex()}", DataType.Double),
          )),
        FeatureSourceDefinition(s"s${nextIndex()}", s"p${nextIndex()}", userFeatureFolder,
          Array(
            FeatureDefinition(s"f${nextIndex()}", DataType.Double),
          )))
    )

    testDataGenerationAndValidate(userFeatureMergeDefinition)
  }

  test("run generate and validate result - string") {
    val userFeatureMergeDefinition = new UserFeatureMergeDefinition(
      "ufmd1", userFeatureFolder, Array(
        FeatureSourceDefinition(s"s${nextIndex()}", s"p${nextIndex()}", userFeatureFolder,
          Array(
            FeatureDefinition(s"f${nextIndex()}", DataType.String),
          )),
        FeatureSourceDefinition(s"s${nextIndex()}", s"p${nextIndex()}", userFeatureFolder,
          Array(
            FeatureDefinition(s"f${nextIndex()}", DataType.String),
          )),
        FeatureSourceDefinition(s"s${nextIndex()}", s"p${nextIndex()}", userFeatureFolder,
          Array(
            FeatureDefinition(s"f${nextIndex()}", DataType.String),
            FeatureDefinition(s"f${nextIndex()}", DataType.String),
          )))
    )

    testDataGenerationAndValidate(userFeatureMergeDefinition)
  }

  test("run generate and validate result - fixed byte array") {
    val userFeatureMergeDefinition = new UserFeatureMergeDefinition(
      "ufmd1", userFeatureFolder, Array(
        FeatureSourceDefinition(s"s${nextIndex()}", s"p${nextIndex()}", userFeatureFolder,
          Array(
            FeatureDefinition(s"f${nextIndex()}", DataType.Byte, 30),
            FeatureDefinition(s"f${nextIndex()}", DataType.Byte, 1),
          )),
        FeatureSourceDefinition(s"s${nextIndex()}", s"p${nextIndex()}", userFeatureFolder,
          Array(
            FeatureDefinition(s"f${nextIndex()}", DataType.Byte, 5),
          )),
        FeatureSourceDefinition(s"s${nextIndex()}", s"p${nextIndex()}", userFeatureFolder,
          Array(
            FeatureDefinition(s"f${nextIndex()}", DataType.Byte, 31),
            FeatureDefinition(s"f${nextIndex()}", DataType.Byte, 1),
            FeatureDefinition(s"f${nextIndex()}", DataType.Byte, 32),
          )))
    )

    testDataGenerationAndValidate(userFeatureMergeDefinition)
    testDataGenerationAndValidate(userFeatureMergeDefinition, enableBinary = false)
  }

  test("run generate and validate result - fixed short array") {
    val userFeatureMergeDefinition = new UserFeatureMergeDefinition(
      "ufmd1", userFeatureFolder, Array(
        FeatureSourceDefinition(s"s${nextIndex()}", s"p${nextIndex()}", userFeatureFolder,
          Array(
            FeatureDefinition(s"f${nextIndex()}", DataType.Short, 2),
            FeatureDefinition(s"f${nextIndex()}", DataType.Short, 1),
          )),
        FeatureSourceDefinition(s"s${nextIndex()}", s"p${nextIndex()}", userFeatureFolder,
          Array(
            FeatureDefinition(s"f${nextIndex()}", DataType.Short, 4),
          )),
        FeatureSourceDefinition(s"s${nextIndex()}", s"p${nextIndex()}", userFeatureFolder,
          Array(
            FeatureDefinition(s"f${nextIndex()}", DataType.Short, 7),
            FeatureDefinition(s"f${nextIndex()}", DataType.Short, 1),
            FeatureDefinition(s"f${nextIndex()}", DataType.Short, 8),
          )))
    )

    testDataGenerationAndValidate(userFeatureMergeDefinition)
  }

  test("run generate and validate result - fixed int array") {
    val userFeatureMergeDefinition = new UserFeatureMergeDefinition(
      "ufmd1", userFeatureFolder, Array(
        FeatureSourceDefinition(s"s${nextIndex()}", s"p${nextIndex()}", userFeatureFolder,
          Array(
            FeatureDefinition(s"f${nextIndex()}", DataType.Int, 7),
          )),
        FeatureSourceDefinition(s"s${nextIndex()}", s"p${nextIndex()}", userFeatureFolder,
          Array(
            FeatureDefinition(s"f${nextIndex()}", DataType.Int, 2),
            FeatureDefinition(s"f${nextIndex()}", DataType.Int, 1),
          )),
        FeatureSourceDefinition(s"s${nextIndex()}", s"p${nextIndex()}", userFeatureFolder,
          Array(
            FeatureDefinition(s"f${nextIndex()}", DataType.Int, 3),
            FeatureDefinition(s"f${nextIndex()}", DataType.Int, 1),
            FeatureDefinition(s"f${nextIndex()}", DataType.Int, 8),
          )))
    )

    testDataGenerationAndValidate(userFeatureMergeDefinition)
  }

  test("run generate and validate result - fixed long array") {
    val userFeatureMergeDefinition = new UserFeatureMergeDefinition(
      "ufmd1", userFeatureFolder, Array(
        FeatureSourceDefinition(s"s${nextIndex()}", s"p${nextIndex()}", userFeatureFolder,
          Array(
            FeatureDefinition(s"f${nextIndex()}", DataType.Long, 1),
            FeatureDefinition(s"f${nextIndex()}", DataType.Long, 2),
          )),
        FeatureSourceDefinition(s"s${nextIndex()}", s"p${nextIndex()}", userFeatureFolder,
          Array(
            FeatureDefinition(s"f${nextIndex()}", DataType.Long, 4),
          )),
        FeatureSourceDefinition(s"s${nextIndex()}", s"p${nextIndex()}", userFeatureFolder,
          Array(
            FeatureDefinition(s"f${nextIndex()}", DataType.Long, 7),
            FeatureDefinition(s"f${nextIndex()}", DataType.Long, 1),
            FeatureDefinition(s"f${nextIndex()}", DataType.Long, 9),
          )))
    )

    testDataGenerationAndValidate(userFeatureMergeDefinition)
  }

  test("run generate and validate result - fixed float array") {
    val userFeatureMergeDefinition = new UserFeatureMergeDefinition(
      "ufmd1", userFeatureFolder, Array(
        FeatureSourceDefinition(s"s${nextIndex()}", s"p${nextIndex()}", userFeatureFolder,
          Array(
            FeatureDefinition(s"f${nextIndex()}", DataType.Float, 3),
            FeatureDefinition(s"f${nextIndex()}", DataType.Float, 1),
          )),
        FeatureSourceDefinition(s"s${nextIndex()}", s"p${nextIndex()}", userFeatureFolder,
          Array(
            FeatureDefinition(s"f${nextIndex()}", DataType.Float, 5),
          )),
        FeatureSourceDefinition(s"s${nextIndex()}", s"p${nextIndex()}", userFeatureFolder,
          Array(
            FeatureDefinition(s"f${nextIndex()}", DataType.Float, 7),
            FeatureDefinition(s"f${nextIndex()}", DataType.Float, 9),
            FeatureDefinition(s"f${nextIndex()}", DataType.Float, 1),
          )))
    )

    testDataGenerationAndValidate(userFeatureMergeDefinition)
  }

  test("run generate and validate result - fixed double array") {
    val userFeatureMergeDefinition = new UserFeatureMergeDefinition(
      "ufmd1", userFeatureFolder, Array(
        FeatureSourceDefinition(s"s${nextIndex()}", s"p${nextIndex()}", userFeatureFolder,
          Array(
            FeatureDefinition(s"f${nextIndex()}", DataType.Double, 3),
            FeatureDefinition(s"f${nextIndex()}", DataType.Double, 1),
          )),
        FeatureSourceDefinition(s"s${nextIndex()}", s"p${nextIndex()}", userFeatureFolder,
          Array(
            FeatureDefinition(s"f${nextIndex()}", DataType.Double, 5),
          )),
        FeatureSourceDefinition(s"s${nextIndex()}", s"p${nextIndex()}", userFeatureFolder,
          Array(
            FeatureDefinition(s"f${nextIndex()}", DataType.Double, 7),
            FeatureDefinition(s"f${nextIndex()}", DataType.Double, 1),
            FeatureDefinition(s"f${nextIndex()}", DataType.Double, 9),
          )))
    )

    testDataGenerationAndValidate(userFeatureMergeDefinition)
  }

  test("run generate and validate result - dynamic byte array") {
    val userFeatureMergeDefinition = new UserFeatureMergeDefinition(
      "ufmd1", userFeatureFolder, Array(
        FeatureSourceDefinition(s"s${nextIndex()}", s"p${nextIndex()}", userFeatureFolder,
          Array(
            FeatureDefinition(s"f${nextIndex()}", DataType.Byte, -1),
          )),
        FeatureSourceDefinition(s"s${nextIndex()}", s"p${nextIndex()}", userFeatureFolder,
          Array(
            FeatureDefinition(s"f${nextIndex()}", DataType.Byte, -1),
          )),
        FeatureSourceDefinition(s"s${nextIndex()}", s"p${nextIndex()}", userFeatureFolder,
          Array(
            FeatureDefinition(s"f${nextIndex()}", DataType.Byte, -1),
            FeatureDefinition(s"f${nextIndex()}", DataType.Byte, -1),
          )))
    )

    testDataGenerationAndValidate(userFeatureMergeDefinition)
    testDataGenerationAndValidate(userFeatureMergeDefinition, enableBinary = false)
  }

  test("run generate and validate result - dynamic short array") {
    val userFeatureMergeDefinition = new UserFeatureMergeDefinition(
      "ufmd1", userFeatureFolder, Array(
        FeatureSourceDefinition(s"s${nextIndex()}", s"p${nextIndex()}", userFeatureFolder,
          Array(
            FeatureDefinition(s"f${nextIndex()}", DataType.Short, -1),
          )),
        FeatureSourceDefinition(s"s${nextIndex()}", s"p${nextIndex()}", userFeatureFolder,
          Array(
            FeatureDefinition(s"f${nextIndex()}", DataType.Short, -1),
          )),
        FeatureSourceDefinition(s"s${nextIndex()}", s"p${nextIndex()}", userFeatureFolder,
          Array(
            FeatureDefinition(s"f${nextIndex()}", DataType.Short, -1),
            FeatureDefinition(s"f${nextIndex()}", DataType.Short, -1),
          )))
    )

    testDataGenerationAndValidate(userFeatureMergeDefinition)
  }

  test("run generate and validate result - dynamic int array") {
    val userFeatureMergeDefinition = new UserFeatureMergeDefinition(
      "ufmd1", userFeatureFolder, Array(
        FeatureSourceDefinition(s"s${nextIndex()}", s"p${nextIndex()}", userFeatureFolder,
          Array(
            FeatureDefinition(s"f${nextIndex()}", DataType.Int, -1),
          )),
        FeatureSourceDefinition(s"s${nextIndex()}", s"p${nextIndex()}", userFeatureFolder,
          Array(
            FeatureDefinition(s"f${nextIndex()}", DataType.Int, -1),
          )),
        FeatureSourceDefinition(s"s${nextIndex()}", s"p${nextIndex()}", userFeatureFolder,
          Array(
            FeatureDefinition(s"f${nextIndex()}", DataType.Int, -1),
            FeatureDefinition(s"f${nextIndex()}", DataType.Int, -1),
          )))
    )

    testDataGenerationAndValidate(userFeatureMergeDefinition)
  }

  test("run generate and validate result - dynamic long array") {
    val userFeatureMergeDefinition = new UserFeatureMergeDefinition(
      "ufmd1", userFeatureFolder, Array(
        FeatureSourceDefinition(s"s${nextIndex()}", s"p${nextIndex()}", userFeatureFolder,
          Array(
            FeatureDefinition(s"f${nextIndex()}", DataType.Long, -1),
          )),
        FeatureSourceDefinition(s"s${nextIndex()}", s"p${nextIndex()}", userFeatureFolder,
          Array(
            FeatureDefinition(s"f${nextIndex()}", DataType.Long, -1),
          )),
        FeatureSourceDefinition(s"s${nextIndex()}", s"p${nextIndex()}", userFeatureFolder,
          Array(
            FeatureDefinition(s"f${nextIndex()}", DataType.Long, -1),
            FeatureDefinition(s"f${nextIndex()}", DataType.Long, -1),
          )))
    )

    testDataGenerationAndValidate(userFeatureMergeDefinition)
  }

  test("run generate and validate result - dynamic float array") {
    val userFeatureMergeDefinition = new UserFeatureMergeDefinition(
      "ufmd1", userFeatureFolder, Array(
        FeatureSourceDefinition(s"s${nextIndex()}", s"p${nextIndex()}", userFeatureFolder,
          Array(
            FeatureDefinition(s"f${nextIndex()}", DataType.Float, -1),
          )),
        FeatureSourceDefinition(s"s${nextIndex()}", s"p${nextIndex()}", userFeatureFolder,
          Array(
            FeatureDefinition(s"f${nextIndex()}", DataType.Float, -1),
          )),
        FeatureSourceDefinition(s"s${nextIndex()}", s"p${nextIndex()}", userFeatureFolder,
          Array(
            FeatureDefinition(s"f${nextIndex()}", DataType.Float, -1),
            FeatureDefinition(s"f${nextIndex()}", DataType.Float, -1),
          )))
    )

    testDataGenerationAndValidate(userFeatureMergeDefinition)
  }

  test("run generate and validate result - dynamic double array") {
    val userFeatureMergeDefinition = new UserFeatureMergeDefinition(
      "ufmd1", userFeatureFolder, Array(
        FeatureSourceDefinition(s"s${nextIndex()}", s"p${nextIndex()}", userFeatureFolder,
          Array(
            FeatureDefinition(s"f${nextIndex()}", DataType.Double, -1),
          )),
        FeatureSourceDefinition(s"s${nextIndex()}", s"p${nextIndex()}", userFeatureFolder,
          Array(
            FeatureDefinition(s"f${nextIndex()}", DataType.Double, -1),
          )),
        FeatureSourceDefinition(s"s${nextIndex()}", s"p${nextIndex()}", userFeatureFolder,
          Array(
            FeatureDefinition(s"f${nextIndex()}", DataType.Double, -1),
            FeatureDefinition(s"f${nextIndex()}", DataType.Double, -1),
          )))
    )

    testDataGenerationAndValidate(userFeatureMergeDefinition)
  }

  test("run generate and validate result") {
    val userFeatureMergeDefinition = new UserFeatureMergeDefinition(
      "ufmd1", userFeatureFolder, Array(
        FeatureSourceDefinition(s"s${nextIndex()}", s"p${nextIndex()}", userFeatureFolder,
          Array(
            FeatureDefinition(s"f${nextIndex()}", DataType.Bool),
            FeatureDefinition(s"f${nextIndex()}", DataType.Bool),
            FeatureDefinition(s"f${nextIndex()}", DataType.Byte),
            FeatureDefinition(s"f${nextIndex()}", DataType.Byte),
            FeatureDefinition(s"f${nextIndex()}", DataType.Float),
            FeatureDefinition(s"f${nextIndex()}", DataType.Double),
            FeatureDefinition(s"f${nextIndex()}", DataType.Byte, 3),
            FeatureDefinition(s"f${nextIndex()}", DataType.Short, -1),
            FeatureDefinition(s"f${nextIndex()}", DataType.Short, 1),
            FeatureDefinition(s"f${nextIndex()}", DataType.Double, -1),
            FeatureDefinition(s"f${nextIndex()}", DataType.String),
          )),
        FeatureSourceDefinition(s"s${nextIndex()}", s"p${nextIndex()}", userFeatureFolder,
          Array(
            FeatureDefinition(s"f${nextIndex()}", DataType.Bool),
            FeatureDefinition(s"f${nextIndex()}", DataType.Short),
            FeatureDefinition(s"f${nextIndex()}", DataType.Short),
            FeatureDefinition(s"f${nextIndex()}", DataType.Int),
            FeatureDefinition(s"f${nextIndex()}", DataType.Int),
            FeatureDefinition(s"f${nextIndex()}", DataType.Float),
            FeatureDefinition(s"f${nextIndex()}", DataType.Short, 5),
            FeatureDefinition(s"f${nextIndex()}", DataType.Int, 7),
            FeatureDefinition(s"f${nextIndex()}", DataType.Int, 1),
            FeatureDefinition(s"f${nextIndex()}", DataType.Int, -1),
            FeatureDefinition(s"f${nextIndex()}", DataType.Float, -1),
            FeatureDefinition(s"f${nextIndex()}", DataType.Float, 3),
            FeatureDefinition(s"f${nextIndex()}", DataType.String),
          )),
        FeatureSourceDefinition(s"s${nextIndex()}", s"p${nextIndex()}", userFeatureFolder,
          Array(
            FeatureDefinition(s"f${nextIndex()}", DataType.Bool),
            FeatureDefinition(s"f${nextIndex()}", DataType.Bool),
            FeatureDefinition(s"f${nextIndex()}", DataType.Int),
            FeatureDefinition(s"f${nextIndex()}", DataType.Long),
            FeatureDefinition(s"f${nextIndex()}", DataType.Long),
            FeatureDefinition(s"f${nextIndex()}", DataType.Double),
            FeatureDefinition(s"f${nextIndex()}", DataType.Long, 3),
            FeatureDefinition(s"f${nextIndex()}", DataType.Short, 3),
            FeatureDefinition(s"f${nextIndex()}", DataType.Short, 1),
            FeatureDefinition(s"f${nextIndex()}", DataType.Long, 9),
            FeatureDefinition(s"f${nextIndex()}", DataType.Float, 11),
            FeatureDefinition(s"f${nextIndex()}", DataType.Double, 2),
            FeatureDefinition(s"f${nextIndex()}", DataType.Double, 1),
            FeatureDefinition(s"f${nextIndex()}", DataType.Double, 13),
            FeatureDefinition(s"f${nextIndex()}", DataType.Byte, -1),
            FeatureDefinition(s"f${nextIndex()}", DataType.Long, -1),
          )))
    )

    testDataGenerationAndValidate(userFeatureMergeDefinition)
    testDataGenerationAndValidate(userFeatureMergeDefinition, enableBinary = false)
  }

  test("run generate and validate result - edge case one boolean feature only") {
    val userFeatureMergeDefinition = new UserFeatureMergeDefinition(
      "ufmd1", userFeatureFolder, Array(
        FeatureSourceDefinition(s"s${nextIndex()}", s"p${nextIndex()}", userFeatureFolder,
          Array(
            FeatureDefinition(s"f${nextIndex()}", DataType.Byte),
            FeatureDefinition(s"f${nextIndex()}", DataType.Byte),
            FeatureDefinition(s"f${nextIndex()}", DataType.Float),
            FeatureDefinition(s"f${nextIndex()}", DataType.Double),
            FeatureDefinition(s"f${nextIndex()}", DataType.Byte, 3),
            FeatureDefinition(s"f${nextIndex()}", DataType.Short, -1),
            FeatureDefinition(s"f${nextIndex()}", DataType.Short, 1),
            FeatureDefinition(s"f${nextIndex()}", DataType.Double, -1),
            FeatureDefinition(s"f${nextIndex()}", DataType.String),
          )),
        FeatureSourceDefinition(s"s${nextIndex()}", s"p${nextIndex()}", userFeatureFolder,
          Array(
            FeatureDefinition(s"f${nextIndex()}", DataType.Bool),
            FeatureDefinition(s"f${nextIndex()}", DataType.Short),
            FeatureDefinition(s"f${nextIndex()}", DataType.Short),
            FeatureDefinition(s"f${nextIndex()}", DataType.Int),
            FeatureDefinition(s"f${nextIndex()}", DataType.Int),
            FeatureDefinition(s"f${nextIndex()}", DataType.Float),
            FeatureDefinition(s"f${nextIndex()}", DataType.Short, 5),
            FeatureDefinition(s"f${nextIndex()}", DataType.Int, 7),
            FeatureDefinition(s"f${nextIndex()}", DataType.Int, 1),
            FeatureDefinition(s"f${nextIndex()}", DataType.Int, -1),
            FeatureDefinition(s"f${nextIndex()}", DataType.Float, -1),
            FeatureDefinition(s"f${nextIndex()}", DataType.Float, 3),
            FeatureDefinition(s"f${nextIndex()}", DataType.Float, 1),
            FeatureDefinition(s"f${nextIndex()}", DataType.String),
          )),
        FeatureSourceDefinition(s"s${nextIndex()}", s"p${nextIndex()}", userFeatureFolder,
          Array(
            FeatureDefinition(s"f${nextIndex()}", DataType.Int),
            FeatureDefinition(s"f${nextIndex()}", DataType.Long),
            FeatureDefinition(s"f${nextIndex()}", DataType.Long),
            FeatureDefinition(s"f${nextIndex()}", DataType.Double),
            FeatureDefinition(s"f${nextIndex()}", DataType.Long, 3),
            FeatureDefinition(s"f${nextIndex()}", DataType.Short, 3),
            FeatureDefinition(s"f${nextIndex()}", DataType.Short, 1),
            FeatureDefinition(s"f${nextIndex()}", DataType.Long, 9),
            FeatureDefinition(s"f${nextIndex()}", DataType.Long, 1),
            FeatureDefinition(s"f${nextIndex()}", DataType.Float, 11),
            FeatureDefinition(s"f${nextIndex()}", DataType.Double, 2),
            FeatureDefinition(s"f${nextIndex()}", DataType.Double, 13),
            FeatureDefinition(s"f${nextIndex()}", DataType.Double, 1),
            FeatureDefinition(s"f${nextIndex()}", DataType.Byte, -1),
            FeatureDefinition(s"f${nextIndex()}", DataType.Long, -1),
          )))
    )

    testDataGenerationAndValidate(userFeatureMergeDefinition)
    testDataGenerationAndValidate(userFeatureMergeDefinition, enableBinary = false)
  }

  override def afterAll(): Unit = {
    super.afterAll()
    FSUtils.deleteDirectory(userFeatureFolder, recursive = true)(spark)
    FSUtils.deleteDirectory(tempFolder, recursive = true)(spark)
  }
}
