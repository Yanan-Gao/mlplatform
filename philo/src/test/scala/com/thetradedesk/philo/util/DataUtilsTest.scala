package com.thetradedesk.philo.util

import org.scalatest.funspec.AnyFunSpec
import org.scalatest.matchers.should.Matchers
import org.scalatest.BeforeAndAfterAll
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions.{col, struct}
import org.apache.spark.sql.types._
import org.apache.spark.sql.Row
import org.apache.spark.sql.types.{IntegerType, StringType, StructField, StructType}

class DataUtilsTest extends AnyFunSpec with Matchers with BeforeAndAfterAll {

  implicit val spark: SparkSession = SparkSession.builder()
    .appName("DataUtilsTest")
    .master("local[*]")
    .config("spark.sql.adaptive.enabled", "false")
    .config("spark.sql.adaptive.coalescePartitions.enabled", "false")
    .getOrCreate()

  import spark.implicits._

  describe("DataUtils") {
    
    describe("flattenData function") {
      it("should extract value field from nested structures") {
        // Create test data with struct columns manually using DataFrame operations
        import org.apache.spark.sql.types._
        import org.apache.spark.sql.Row
        
        val schema = StructType(Seq(
          StructField("id", IntegerType, false),
          StructField("DeviceType", StructType(Seq(
            StructField("value", StringType, true)
          )), true),
          StructField("regular_col", StringType, true)
        ))
        
        val data = Seq(
          Row(1, Row("device1"), "test1"),
          Row(2, Row("device2"), "test2")
        )
        
        val testData = spark.createDataFrame(spark.sparkContext.parallelize(data), schema)
        val flattenSet = Set("DeviceType")
        val result = DataUtils.flattenData(testData, flattenSet)

        // Should have the same columns but DeviceType should now contain the value
        val resultColumns = result.columns.toSet
        resultColumns should contain("id")
        resultColumns should contain("regular_col")
        resultColumns should contain("DeviceType")
        
        result.count() shouldBe testData.count()
      }

      it("should handle empty flatten set") {
        val testData = Seq(
          (1, "test1"),
          (2, "test2")
        ).toDF("id", "name")

        val result = DataUtils.flattenData(testData, Set.empty)
        
        result.columns should contain theSameElementsAs testData.columns
        result.count() shouldBe testData.count()
      }

      it("should handle non-existent columns in flatten set") {
        val testData = Seq(
          (1, "test1"),
          (2, "test2")
        ).toDF("id", "name")

        val flattenSet = Set("NonExistentColumn")
        val result = DataUtils.flattenData(testData, flattenSet)
        
        // Should return original data when flatten column doesn't exist
        result.columns should contain theSameElementsAs testData.columns
        result.count() shouldBe testData.count()
      }
    }

    describe("addOriginalCols function") {
      it("should add original column names with prefix") {
        val testData = Seq(
          (1, "John", 25),
          (2, "Jane", 30)
        ).toDF("id", "name", "age")

        val keptCols = Seq("name", "age")
        val (resultDf, originalColNames) = DataUtils.addOriginalCols(keptCols, testData)

        // Check that original columns are renamed with 'original' prefix
        val resultColumns = resultDf.columns.toSet
        resultColumns should contain("originalname")
        resultColumns should contain("originalage")
        resultColumns should contain("id") // non-kept column remains unchanged

        // Check returned original column names
        originalColNames should contain theSameElementsAs Seq("originalname", "originalage")

        // Verify data integrity
        val originalData = resultDf.select("originalname", "originalage").collect()
        originalData(0).getString(0) shouldBe "John"
        originalData(0).getInt(1) shouldBe 25
        originalData(1).getString(0) shouldBe "Jane"
        originalData(1).getInt(1) shouldBe 30
      }

      it("should handle empty kept columns") {
        val testData = Seq(
          (1, "John"),
          (2, "Jane")
        ).toDF("id", "name")

        val (resultDf, originalColNames) = DataUtils.addOriginalCols(Seq.empty, testData)

        // Should return original DataFrame unchanged
        resultDf.columns should contain theSameElementsAs testData.columns
        originalColNames shouldBe empty
      }

      it("should handle non-existent columns by throwing exception") {
        val testData = Seq(
          (1, "John"),
          (2, "Jane")
        ).toDF("id", "name")

        val keptCols = Seq("name", "nonexistent")
        
        // Should throw exception when trying to reference non-existent column
        an[Exception] should be thrownBy {
          DataUtils.addOriginalCols(keptCols, testData)
        }
      }
    }

    describe("debugInfo function") {
      it("should not throw errors when called") {
        val testData = Seq(
          (1, "test1"),
          (2, "test2")
        ).toDF("id", "name")

        // debugInfo should complete without throwing exceptions
        noException should be thrownBy {
          DataUtils.debugInfo("test_dataset", testData)
        }
      }

      it("should handle empty datasets") {
        val emptyData = spark.emptyDataFrame

        noException should be thrownBy {
          DataUtils.debugInfo("empty_dataset", emptyData)
        }
      }
    }
  }

  override def afterAll(): Unit = {
    spark.stop()
  }
}