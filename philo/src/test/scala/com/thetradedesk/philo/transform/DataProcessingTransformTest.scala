package com.thetradedesk.philo.transform

import org.scalatest.funspec.AnyFunSpec
import org.scalatest.matchers.should.Matchers
import org.scalatest.BeforeAndAfterAll
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions.{col, lit, size, array}
import org.apache.spark.sql.types._
import org.apache.spark.sql.Row
import com.thetradedesk.geronimo.bidsimpression.schema.BidsImpressionsSchema
import com.thetradedesk.philo.schema.ClickTrackerRecord

class DataProcessingTransformTest extends AnyFunSpec with Matchers with BeforeAndAfterAll {

  implicit val spark: SparkSession = SparkSession.builder()
    .appName("DataProcessingTransformTest")
    .master("local[*]")
    .config("spark.sql.adaptive.enabled", "false")
    .config("spark.sql.adaptive.coalescePartitions.enabled", "false")
    .getOrCreate()

  import spark.implicits._

  describe("DataProcessingTransform") {
    
    describe("addUserDataFeatures function") {
      it("should add user data features without bot filtering") {
        import org.apache.spark.sql.types._
        import org.apache.spark.sql.Row
        
        val schema = StructType(Seq(
          StructField("id", IntegerType, false),
          StructField("MatchedSegments", ArrayType(IntegerType), true),
          StructField("UserSegmentCount", IntegerType, true)
        ))
        
        val data = Seq(
          Row(1, Array(1001, 1002, 1003), 3),
          Row(2, Array.empty[Int], 0),
          Row(3, null, null)
        )
        
        val testData = spark.createDataFrame(spark.sparkContext.parallelize(data), schema)

        val result = DataProcessingTransform.addUserDataFeatures(testData, filterClickBots = false)
        val resultData = result.collect()

        // Check HasUserData column
        resultData(0).getInt(result.columns.indexOf("HasUserData")) shouldBe 1  // Has segments
        resultData(1).getInt(result.columns.indexOf("HasUserData")) shouldBe 0  // Empty segments
        resultData(2).getInt(result.columns.indexOf("HasUserData")) shouldBe 0  // Null segments

        // Check UserDataLength column
        resultData(0).getDouble(result.columns.indexOf("UserDataLength")) shouldBe 3.0
        resultData(1).getDouble(result.columns.indexOf("UserDataLength")) shouldBe 0.0
        resultData(2).getDouble(result.columns.indexOf("UserDataLength")) shouldBe 0.0

        // Check UserDataOptIn column (should always be 1)
        resultData.foreach { row =>
          row.getInt(result.columns.indexOf("UserDataOptIn")) shouldBe 1
        }
      }

      it("should handle null MatchedSegments") {
        import org.apache.spark.sql.types._
        import org.apache.spark.sql.Row
        
        val schema = StructType(Seq(
          StructField("id", IntegerType, false),
          StructField("MatchedSegments", ArrayType(IntegerType), true),
          StructField("UserSegmentCount", IntegerType, true)
        ))
        
        val data = Seq(Row(1, null, null))
        val testData = spark.createDataFrame(spark.sparkContext.parallelize(data), schema)

        val result = DataProcessingTransform.addUserDataFeatures(testData, filterClickBots = false)
        val resultData = result.collect()

        resultData(0).getInt(result.columns.indexOf("HasUserData")) shouldBe 0
        resultData(0).getDouble(result.columns.indexOf("UserDataLength")) shouldBe 0.0
        Option(resultData(0).get(result.columns.indexOf("UserData"))) shouldBe None
      }

      it("should set UserData to null when HasUserData is 0") {
        val testData = Seq(
          (1, Array.empty[Int], 0)  // Empty segments
        ).toDF("id", "MatchedSegments", "UserSegmentCount")

        val result = DataProcessingTransform.addUserDataFeatures(testData, filterClickBots = false)
        val resultData = result.collect()

        resultData(0).getInt(result.columns.indexOf("HasUserData")) shouldBe 0
        Option(resultData(0).get(result.columns.indexOf("UserData"))) shouldBe None
      }

      it("should set UserData to MatchedSegments when HasUserData is 1") {
        val testData = Seq(
          (1, Array(1001, 1002), 2)
        ).toDF("id", "MatchedSegments", "UserSegmentCount")

        val result = DataProcessingTransform.addUserDataFeatures(testData, filterClickBots = false)
        val resultData = result.collect()

        resultData(0).getInt(result.columns.indexOf("HasUserData")) shouldBe 1
        val userData = resultData(0).getSeq[Int](result.columns.indexOf("UserData"))
        userData should contain allOf(1001, 1002)
      }

      it("should apply bot filtering when requested") {
        // Create test data that would trigger bot filtering
        val testData = Seq(
          ("user1", 1, Array(1001), 1),
          ("user2", 1, Array(1002), 1)
        ).toDF("UIID", "label", "MatchedSegments", "UserSegmentCount")

        // Note: Bot filtering requires specific data structure, so this test
        // mainly verifies that the function calls BotFilterTransform.filterBots
        val result = DataProcessingTransform.addUserDataFeatures(testData, filterClickBots = true)
        
        // Should complete without errors and add user data features
        result.columns should contain("HasUserData")
        result.columns should contain("UserDataLength")
        result.columns should contain("UserData")
        result.columns should contain("UserDataOptIn")
      }

      it("should handle missing UserSegmentCount column") {
        val testData = Seq(
          (1, Array(1001, 1002))
        ).toDF("id", "MatchedSegments")
          .withColumn("UserSegmentCount", lit(null).cast("int"))

        val result = DataProcessingTransform.addUserDataFeatures(testData, filterClickBots = false)
        val resultData = result.collect()

        // Should still work with null UserSegmentCount
        resultData(0).getInt(result.columns.indexOf("HasUserData")) shouldBe 1
        resultData(0).getDouble(result.columns.indexOf("UserDataLength")) shouldBe 0.0
      }
    }
  }

  override def afterAll(): Unit = {
    spark.stop()
  }
}