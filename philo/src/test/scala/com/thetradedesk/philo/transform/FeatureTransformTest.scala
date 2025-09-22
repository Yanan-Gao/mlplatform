package com.thetradedesk.philo.transform

import org.scalatest.funspec.AnyFunSpec
import org.scalatest.matchers.should.Matchers
import org.scalatest.BeforeAndAfterAll
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions.{col, lit, xxhash64}
import org.apache.spark.sql.types.{ArrayType, IntegerType, LongType, StringType, StructField, StructType}
import com.thetradedesk.geronimo.shared._
import com.thetradedesk.geronimo.shared.schemas.ModelFeature

class FeatureTransformTest extends AnyFunSpec with Matchers with BeforeAndAfterAll {

  implicit val spark: SparkSession = SparkSession.builder()
    .appName("FeatureTransformTest")
    .master("local[*]")
    .config("spark.sql.adaptive.enabled", "false")
    .config("spark.sql.adaptive.coalescePartitions.enabled", "false")
    .getOrCreate()

  import spark.implicits._

  describe("FeatureTransform") {
    
    describe("intModelFeaturesCols function") {
      it("should transform string features with hashing") {
        val features = Seq(
          ModelFeature("string_feature", STRING_FEATURE_TYPE, Some(100), 0, None, None)
        )

        val testData = Seq(
          "value1",
          "value2",
          null
        ).toDF("string_feature")

        val columns = FeatureTransform.intModelFeaturesCols(features)
        val result = testData.select(columns: _*)

        result.columns should contain("string_feature")
        val resultData = result.collect()
        
        // Non-null values should be hashed and shifted
        resultData(0).getInt(0) should be > 0
        resultData(1).getInt(0) should be > 0
        
        // Null values should remain as 0
        resultData(2).getInt(0) shouldBe 0
      }

      it("should transform integer features with modulo") {
        val features = Seq(
          ModelFeature("int_feature", INT_FEATURE_TYPE, Some(50), 0, None, None)
        )

        val testData = Seq(
          Option(10),
          Option(25),
          None
        ).toDF("int_feature")

        val columns = FeatureTransform.intModelFeaturesCols(features)
        val result = testData.select(columns: _*)

        result.columns should contain("int_feature")
        val resultData = result.collect()
        
        // Non-null values should be processed
        resultData(0).getInt(0) should be > 0
        resultData(1).getInt(0) should be > 0
        
        // Null values should remain as 0
        resultData(2).getInt(0) shouldBe 0
      }

      it("should pass through float features unchanged") {
        val features = Seq(
          ModelFeature("float_feature", FLOAT_FEATURE_TYPE, None, 0, None, None)
        )

        val testData = Seq(
          Option(1.5f),
          Option(2.7f),
          None
        ).toDF("float_feature")

        val columns = FeatureTransform.intModelFeaturesCols(features)
        val result = testData.select(columns: _*)

        result.columns should contain("float_feature")
        val resultData = result.collect()
        
        // Float values should be unchanged
        resultData(0).getFloat(0) shouldBe 1.5f
        resultData(1).getFloat(0) shouldBe 2.7f
        Option(resultData(2).get(0)) shouldBe None
      }

      it("should handle mixed feature types") {
        val features = Seq(
          ModelFeature("string_feature", STRING_FEATURE_TYPE, Some(100), 0, None, None),
          ModelFeature("int_feature", INT_FEATURE_TYPE, Some(50), 0, None, None),
          ModelFeature("float_feature", FLOAT_FEATURE_TYPE, None, 0, None, None)
        )

        val testData = Seq(
          ("value1", 10, 1.5f),
          ("value2", 20, 2.5f)
        ).toDF("string_feature", "int_feature", "float_feature")

        val columns = FeatureTransform.intModelFeaturesCols(features)
        val result = testData.select(columns: _*)

        result.columns should have length 3
        result.columns should contain allOf("string_feature", "int_feature", "float_feature")
        result.count() shouldBe 2
      }
    }


    describe("getHashedData function") {
      it("should combine feature processing with original columns") {
        val hashFeatures = Seq(
          ModelFeature("string_feature", STRING_FEATURE_TYPE, Some(100), 0, None, None)
        )
        val seqHashFields = Seq.empty[ModelFeature]
        val originalColNames = Seq("original_col")
        val addCols = Seq("label")

        val testData = Seq(
          ("value1", "orig1", 1),
          ("value2", "orig2", 0)
        ).toDF("string_feature", "original_col", "label")

        val result = FeatureTransform.getHashedData(
          testData, hashFeatures, seqHashFields, originalColNames, addCols
        )

        val resultColumns = result.columns.toSet
        resultColumns should contain("string_feature") // processed feature
        resultColumns should contain("original_col")   // original column
        resultColumns should contain("label")          // additional column
        
        result.count() shouldBe 2
      }
    }

    describe("maskFeatures function") {
      it("should mask features when IsRestricted is 1") {
        val testData = Seq(
          (100, 0), // Not restricted
          (200, 1), // Restricted
          (300, 0)  // Not restricted
        ).toDF("feature_value", "IsRestricted")

        val result = FeatureTransform.maskFeatures(testData, "feature_value")
        val resultData = result.collect()

        // First and third rows should retain values
        resultData(0).getInt(0) shouldBe 100
        resultData(2).getInt(0) shouldBe 300

        // Second row should be masked to 0
        resultData(1).getInt(0) shouldBe 0
      }
    }
  }

  override def afterAll(): Unit = {
    spark.stop()
  }
}