package com.thetradedesk.kongming.datasets

import com.thetradedesk.spark.TTDSparkContext
import com.thetradedesk.spark.datasets.core._
import com.thetradedesk.spark.util.TTDConfig.environment
import com.thetradedesk.spark.util.{ProdTesting, Production, Testing}
import org.apache.spark.sql.SparkSession
import org.scalatest.funsuite.AnyFunSuite

class MetastoreTraitTest extends AnyFunSuite {

  val reader: MetastoreHandler = new MetastoreHandler {
    environment = Production
    override val datasetTypeMh: DataSetType = SourceDataSet
    override val isInChainMh: Boolean = false
    override val tableName: String = ""
    override val metastorePartitionField1: String = ""
    override val isStrictCaseClassSchema: Boolean = true
    override val isExperimentMh: Boolean = false
    override val supportMetastore: Boolean = true
  }


  test("dbName returns correct value for Production and cpa task") {
    val dbName = reader.getDbNameForRead(SourceDataSet, isInChain = false, "cpa")
    assert(dbName == reader.CPA)
  }

  test("returns roas for Production and roas task") {
    environment = Production
    val dbName = reader.getDbNameForRead(SourceDataSet, isInChain = false, "roas")
    assert(dbName == reader.ROAS)
  }

  test("throws exception for unsupported read task") {
    environment = Production
    val ex = intercept[IllegalArgumentException] {
      reader.getDbNameForRead(SourceDataSet, isInChain = false, "unknown")
    }
    assert(ex.getMessage.contains("Unknown task name"))
  }

  test("returns cpa_test for ProdTesting and isInChain = true") {
    environment = ProdTesting
    val dbName = reader.getDbNameForRead(GeneratedDataSet, isInChain = true, "cpa")
    assert(dbName == reader.CPA_TEST)
  }

  test("returns roas_test for ProdTesting and isInChain = true") {
    environment = ProdTesting
    val dbName = reader.getDbNameForRead(SourceDataSet, isInChain = true, "roas")
    assert(dbName == reader.ROAS_TEST)
  }

  test("returns roas for Testing and SourceDataSet") {
    environment = Testing
    val dbName = reader.getDbNameForRead(SourceDataSet, isInChain = false, "roas")
    assert(dbName == reader.ROAS)
  }

  test("returns roas_test for Testing and GeneratedDataSet") {
    environment = Testing
    val dbName = reader.getDbNameForRead(GeneratedDataSet, isInChain = true, "roas")
    assert(dbName == reader.ROAS_TEST)
  }

  test("returns cpa for ProdTesting and isInChain = false") {
    environment = ProdTesting
    val dbName = reader.getDbNameForRead(SourceDataSet, isInChain = false, "cpa")
    assert(dbName == reader.CPA)
  }


  test("returns cpa for Production and cpa task") {
    environment = Production
    val dbName = reader.getDbNameForWrite("cpa")
    assert(dbName == reader.CPA)
  }

  test("returns cpa_test for ProdTesting and cpa task") {
    environment = ProdTesting
    val dbName = reader.getDbNameForWrite("cpa")
    assert(dbName == reader.CPA_TEST)
  }

  test("returns roas_test for Testing and roas task") {
    environment = Testing
    val dbName = reader.getDbNameForWrite("roas")
    assert(dbName == reader.ROAS_TEST)
  }

  test("throws exception for unsupported write task") {
    environment = Production
    val ex = intercept[IllegalArgumentException] {
      reader.getDbNameForWrite("unknown")
    }
    assert(ex.getMessage.contains("Unknown task name"))
  }

  implicit val spark: SparkSession = SparkSession.builder()
    .appName("Test")
    .master("local[*]")
    .getOrCreate()

  import spark.implicits._

  test("extractPartitionValues returns correct values") {
    val rawPartitions = Seq(
      "date=20250705/split=train",
      "date=20250705/split=test",
      "date=20250706/split=train",
      "date=20250706/split=test",
      "date=20250707/split=test"
    )
    val df = rawPartitions.toDF("partition")

    val result = reader.extractPartitionValues(df, "date")
    assert(result == Set("20250705", "20250706", "20250707"))
  }

  test("extractPartitionValues ignores irrelevant fields") {
    val rawPartitions = Seq(
      "date=20250705/split=20250704",
      "date=20250705/split=20250705",
      "date=20250706/split=20250705",
      "date=20250706/split=20250706",
      "date=20250707/split=20250706"
    )
    val df = rawPartitions.toDF("partition")

    val result = reader.extractPartitionValues(df, "date")
    assert(result == Set("20250705", "20250706", "20250707"))
  }

  test("extractPartitionValues handles missing date fields gracefully") {
    val rawPartitions = Seq(
      "delay=10D",
      "region=Asia"
    )
    val df = rawPartitions.toDF("partition")

    val result = reader.extractPartitionValues(df, "date")
    assert(result.isEmpty)
  }

  test("filterAndExtractPartitionValues filters partitions and extracts correct field") {
    val partitions = Seq(
      "delay=10D/date=20250705",
      "delay=14D/date=20250706",
      "delay=10D/date=20250707"
    )

    val result = reader.filterAndExtractPartitionValues(
      partitions,
      partitionField = "date",
      extraConditionField = "delay",
      extraConditionValue = "10D"
    )

    assert(result == Set("20250705", "20250707"))
  }

  test("filterAndExtractPartitionValues returns empty set when no partitions match condition") {
    val partitions = Seq(
      "delay=14D/date=20250704",
      "delay=2D/date=20250705"
    )

    val result = reader.filterAndExtractPartitionValues(
      partitions,
      partitionField = "date",
      extraConditionField = "delay",
      extraConditionValue = "10D"
    )

    assert(result.isEmpty)
  }


  test("sanitizeString should replace special characters with underscores") {
    val input = "data_name=aria.li_date=2025-8-12"
    val expected = "data_name_aria_li_date_2025_8_12"
    val result = reader.sanitizeString(input)
    assert(result == expected)
  }

}
