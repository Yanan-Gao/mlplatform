package com.thetradedesk.philo.util

import org.scalatest.funspec.AnyFunSpec
import org.scalatest.matchers.should.Matchers
import org.scalatest.BeforeAndAfterAll
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions.col
import com.thetradedesk.geronimo.shared.schemas.ModelFeature
import com.thetradedesk.geronimo.shared._

class FeatureUtilsTest extends AnyFunSpec with Matchers with BeforeAndAfterAll {

  implicit val spark: SparkSession = SparkSession.builder()
    .appName("FeatureUtilsTest")
    .master("local[*]")
    .config("spark.sql.adaptive.enabled", "false")
    .config("spark.sql.adaptive.coalescePartitions.enabled", "false")
    .getOrCreate()

  import spark.implicits._

  describe("FeatureUtils") {
    
    describe("aliasedModelFeatureNames function") {
      it("should return feature names from ModelFeature array") {
        val features = Array(
          ModelFeature("feature1", STRING_FEATURE_TYPE, Some(100), 0, None, None),
          ModelFeature("feature2", INT_FEATURE_TYPE, Some(50), 0, None, None),
          ModelFeature("feature3", FLOAT_FEATURE_TYPE, None, 0, None, None)
        )

        val result = FeatureUtils.aliasedModelFeatureNames(features)
        
        result should have length 3
        result should contain("feature1")
        result should contain("feature2")
        result should contain("feature3")
      }

      it("should handle empty feature array") {
        val features = Array.empty[ModelFeature]
        val result = FeatureUtils.aliasedModelFeatureNames(features)
        
        result shouldBe empty
      }

      it("should preserve order of features") {
        val features = Array(
          ModelFeature("alpha", STRING_FEATURE_TYPE, Some(100), 0, None, None),
          ModelFeature("beta", INT_FEATURE_TYPE, Some(50), 0, None, None),
          ModelFeature("gamma", FLOAT_FEATURE_TYPE, None, 0, None, None)
        )

        val result = FeatureUtils.aliasedModelFeatureNames(features)
        
        result.toSeq shouldBe Seq("alpha", "beta", "gamma")
      }
    }

    describe("addOriginalNames function") {
      it("should add 'original' prefix to column names") {
        val keptCols = Seq("col1", "col2", "col3")
        val result = FeatureUtils.addOriginalNames(keptCols)
        
        result should have length 3
        result should contain("originalcol1")
        result should contain("originalcol2")
        result should contain("originalcol3")
      }

      it("should handle empty column list") {
        val result = FeatureUtils.addOriginalNames(Seq.empty)
        result shouldBe empty
      }

      it("should preserve order") {
        val keptCols = Seq("first", "second", "third")
        val result = FeatureUtils.addOriginalNames(keptCols)
        
        result.toSeq shouldBe Seq("originalfirst", "originalsecond", "originalthird")
      }

      it("should handle special characters in column names") {
        val keptCols = Seq("col_with_underscore", "col-with-dash", "col.with.dot")
        val result = FeatureUtils.addOriginalNames(keptCols)
        
        result should contain("originalcol_with_underscore")
        result should contain("originalcol-with-dash")
        result should contain("originalcol.with.dot")
      }
    }
  }

  override def afterAll(): Unit = {
    spark.stop()
  }
}