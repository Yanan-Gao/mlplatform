package com.thetradedesk.plutus.data.utils

import com.thetradedesk.plutus.data.transform.campaignbackoff.HadesCampaignAdjustmentsTransform.Campaign
import org.apache.spark.sql.SparkSession
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers

import java.nio.file.{Files, Paths}
import java.time.LocalDate
import java.util.Comparator
import scala.jdk.CollectionConverters.asScalaIteratorConverter

class S3DailyParquetDatasetTest extends AnyFunSuite with Matchers {

  implicit val spark: SparkSession = SparkSession.builder().master("local").getOrCreate()
  import spark.implicits._
  val localPrefix = "test-data"
  val testEnv = "test-env"

  // Mock implementation of the abstract class with exposed protected methods
  class TestS3DailyParquetDataset extends S3DailyParquetDataset[Campaign] {
    override protected def genBasePath(env: String): String = s"$localPrefix/$env"

    // Expose protected methods for testing
    def testGenBasePath(env: String): String = genBasePath(env)
    def testGenDateSuffix(date: LocalDate): String = genDateSuffix(date)
    def testGenPathForDate(date: LocalDate, env: String): String = genPathForDate(date, env)
    def testGenPathsForDateWithLookback(date: LocalDate, lookBack: Int, env: String): Seq[String] = genPathsForDateWithLookback(date, lookBack, env)
    def testExtractDateFromPath(path: String): Option[LocalDate] = extractDateFromPath(path)
  }

  val dataset = new TestS3DailyParquetDataset

  test("genDateSuffix should generate correct date suffix") {
    val date = LocalDate.of(2023, 1, 15)
    val suffix = dataset.testGenDateSuffix(date)
    suffix shouldEqual "date=20230115"
  }

  test("genPathForDate should generate correct S3 path") {
    val date = LocalDate.of(2023, 1, 15)
    val path = dataset.testGenPathForDate(date, testEnv)
    path shouldEqual f"$localPrefix/$testEnv/date=20230115"
  }

  test("genPathsForDateWithLookback should generate correct paths with lookback") {
    val date = LocalDate.of(2023, 1, 15)
    val paths = dataset.testGenPathsForDateWithLookback(date, 2, testEnv)
    paths shouldEqual Seq(
      f"$localPrefix/$testEnv/date=20230115",
      f"$localPrefix/$testEnv/date=20230114",
      f"$localPrefix/$testEnv/date=20230113"
    )
  }

  test("extractDateFromPath should extract date from path") {
    val path = f"$localPrefix/$testEnv/date=20230115"
    val date = dataset.testExtractDateFromPath(path)
    date shouldEqual Some(LocalDate.of(2023, 1, 15))
  }

  test("extractDateFromPath should return None for invalid path") {
    val path = f"$localPrefix/$testEnv/no-date"
    val date = dataset.testExtractDateFromPath(path)
    date shouldEqual None
  }

  test("readLatestDataUpToIncluding should read dataset up to including max date") {
    val maxDate = LocalDate.of(2023, 1, 15)

    // Create test data
    val data1 = Seq(Campaign("a")).toDS()
    val data2 = Seq(Campaign("b")).toDS()
    val data3 = Seq(Campaign("c")).toDS()
    val data4 = Seq(Campaign("d")).toDS()

    // Write test data to local paths
    dataset.writeData(maxDate, data1, env=testEnv)
    dataset.writeData(maxDate.minusDays(1), data2, env=testEnv)
    dataset.writeData(maxDate.minusDays(4), data3, env=testEnv)
    dataset.writeData(maxDate.minusDays(5), data4, env=testEnv)

    // ----------------------------------------
    // TEST 1: With Full lookback
    // ----------------------------------------
    var result = dataset.readLatestDataUpToIncluding(maxDate, testEnv, 2)
    var expectedData = data1.union(data2).union(data3)

    result.collect() should contain theSameElementsAs expectedData.collect()

    // ----------------------------------------
    // TEST 2: With partial lookback
    // ----------------------------------------
    result = dataset.readLatestDataUpToIncluding(maxDate, testEnv, 1)
    expectedData = data1.union(data2)

    result.collect() should contain theSameElementsAs expectedData.collect()

    // ----------------------------------------
    // TEST 3: With no lookback
    // ----------------------------------------
    result = dataset.readLatestDataUpToIncluding(maxDate, testEnv)
    expectedData = data1

    result.collect() should contain theSameElementsAs expectedData.collect()

    // ----------------------------------------
    // TEST 4: With advanced date no lookback
    // ----------------------------------------
    result = dataset.readLatestDataUpToIncluding(maxDate.plusDays(3), testEnv)
    expectedData = data1

    result.collect() should contain theSameElementsAs expectedData.collect()

    // ----------------------------------------
    // TEST 5: With mid-line date some lookback
    // ----------------------------------------
    result = dataset.readLatestDataUpToIncluding(maxDate.minusDays(1), testEnv, 1)
    expectedData = data2.union(data3)

    result.collect() should contain theSameElementsAs expectedData.collect()

    // ----------------------------------------
    // TEST 6: With way past date
    // ----------------------------------------
    assertThrows[S3NoFilesFoundException]{
      dataset.readLatestDataUpToIncluding(maxDate.minusDays(6), testEnv, 1)
    }

    // We expect an empty result

    // Clean up the written data
    Files.walk(Paths.get(localPrefix))
      .sorted(Comparator.reverseOrder())
      .iterator()
      .asScala
      .map(_.toFile)
      .foreach(_.delete())
  }
}