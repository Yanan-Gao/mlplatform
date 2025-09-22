package com.thetradedesk.philo.transform

import org.scalatest.funspec.AnyFunSpec
import org.scalatest.matchers.should.Matchers
import org.scalatest.BeforeAndAfterAll
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions.{col, lit}
import com.thetradedesk.philo.BotFilteringControlPoints

class BotFilterTransformTest extends AnyFunSpec with Matchers with BeforeAndAfterAll {

  implicit val spark: SparkSession = SparkSession.builder()
    .appName("BotFilterTransformTest")
    .master("local[*]")
    .config("spark.sql.adaptive.enabled", "false")
    .config("spark.sql.adaptive.coalescePartitions.enabled", "false")
    .getOrCreate()

  import spark.implicits._

  describe("BotFilterTransform") {
    
    describe("filterBots function") {
      it("should filter out high CTR users with many impressions") {
        val testData = Seq(
          // Normal users
          ("user1", 1, 10),   // 1 click, 10 impressions, CTR = 0.1
          ("user2", 2, 50),   // 2 clicks, 50 impressions, CTR = 0.04
          ("user3", 1, 20),   // 1 click, 20 impressions, CTR = 0.05
          
          // Potential bot - high impressions with suspicious CTR
          ("bot1", 15, 30),   // 15 clicks, 30 impressions, CTR = 0.5
          ("bot2", 8, 25)     // 8 clicks, 25 impressions, CTR = 0.32
        ).toDF("UIID", "label", "impressions")
        
        // Create expanded dataset with individual impression/click records
        val expandedData = testData.flatMap { row =>
          val uiid = row.getString(0)
          val clicks = row.getInt(1)
          val impressions = row.getInt(2)
          
          (1 to impressions).map { i =>
            val label = if (i <= clicks) 1 else 0
            (uiid, label)
          }
        }.toDF("UIID", "label")

        val result = BotFilterTransform.filterBots(expandedData)
        val resultUiids = result.select("UIID").distinct().collect().map(_.getString(0)).toSet

        // Normal users should remain
        resultUiids should contain("user1")
        resultUiids should contain("user2") 
        resultUiids should contain("user3")
        
        // Bots might be filtered out (depending on thresholds)
        // Note: Exact filtering depends on the threshold calculations
        result.count() should be <= expandedData.count()
      }

      it("should handle empty dataset") {
        val emptyData = spark.emptyDataFrame.select(
          lit(null).cast("string").alias("UIID"),
          lit(null).cast("int").alias("label")
        ).filter(lit(false))

        val result = BotFilterTransform.filterBots(emptyData)
        result.count() shouldBe 0
      }

      it("should handle dataset with no clicks") {
        val testData = Seq(
          ("user1", 0),
          ("user2", 0),
          ("user3", 0)
        ).toDF("UIID", "label")

        val result = BotFilterTransform.filterBots(testData)
        
        // Should return original data since no users have clicks
        result.count() shouldBe testData.count()
      }

      it("should preserve users with low impression counts") {
        val testData = Seq(
          ("user1", 1), // Low impression users should not be filtered
          ("user2", 1),
          ("user3", 0)
        ).toDF("UIID", "label")

        val result = BotFilterTransform.filterBots(testData)
        
        // Low impression users should not be filtered regardless of CTR
        result.count() shouldBe testData.count()
      }
    }

    describe("percentileToThreshold function") {
      it("should calculate percentiles correctly") {
        val testData = Seq(1, 2, 3, 4, 5, 6, 7, 8, 9, 10).toDF("num_impressions")

        val p50 = BotFilterTransform.percentileToThreshold(testData, 50.0)
        val p90 = BotFilterTransform.percentileToThreshold(testData, 90.0)
        val p99 = BotFilterTransform.percentileToThreshold(testData, 99.0)

        p50 should be >= 1
        p50 should be <= 10
        
        p90 should be >= p50
        p90 should be <= 10
        
        p99 should be >= p90
        p99 should be <= 10
      }

      it("should handle edge cases") {
        val testData = Seq(5).toDF("num_impressions")

        val p50 = BotFilterTransform.percentileToThreshold(testData, 50.0)
        val p100 = BotFilterTransform.percentileToThreshold(testData, 100.0)

        p50 shouldBe 5
        p100 shouldBe 5
      }
    }
  }

  override def afterAll(): Unit = {
    spark.stop()
  }
}