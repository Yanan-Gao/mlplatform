package com.thetradedesk.philo.transform

import org.scalatest.funspec.AnyFunSpec
import org.scalatest.matchers.should.Matchers
import org.scalatest.BeforeAndAfterAll
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions.{col, lit}
import com.thetradedesk.philo.schema.{AdGroupRecord, PartnerExclusionRecord, SensitiveAdvertiserRecord, CreativeLandingPageRecord, CountryFilterRecord}

class FilterTransformTest extends AnyFunSpec with Matchers with BeforeAndAfterAll {

  implicit val spark: SparkSession = SparkSession.builder()
    .appName("FilterTransformTest")
    .master("local[*]")
    .config("spark.sql.adaptive.enabled", "false")
    .config("spark.sql.adaptive.coalescePartitions.enabled", "false")
    .getOrCreate()

  import spark.implicits._

  describe("FilterTransform") {
    
    describe("addExclusionFlag function") {
      it("should add exclusion flags based on partner exclusion list") {
        val testData = Seq(
          (1, 100),
          (2, 200),
          (3, 300),
          (4, 400)
        ).toDF("id", "PartnerId")

        val exclusionList = Seq(
          PartnerExclusionRecord("200"),
          PartnerExclusionRecord("400")
        ).toDS()

        val result = FilterTransform.addExclusionFlag(testData, Some(exclusionList))
        val resultData = result.select("id", "PartnerId", "excluded").collect()

        // Check that correct partners are marked as excluded
        val excludedMap = resultData.map(row => row.getInt(1) -> row.getInt(2)).toMap
        excludedMap(100) shouldBe 0  // Not excluded
        excludedMap(200) shouldBe 1  // Excluded
        excludedMap(300) shouldBe 0  // Not excluded
        excludedMap(400) shouldBe 1  // Excluded
      }

      it("should mark all as not excluded when no exclusion list provided") {
        val testData = Seq(
          (1, 100),
          (2, 200)
        ).toDF("id", "PartnerId")

        val result = FilterTransform.addExclusionFlag(testData, None)
        val resultData = result.select("excluded").collect()

        // All should be marked as not excluded (0)
        resultData.foreach(_.getInt(0) shouldBe 0)
      }

      it("should handle empty exclusion list") {
        val testData = Seq(
          (1, 100),
          (2, 200)
        ).toDF("id", "PartnerId")

        val emptyExclusionList = spark.emptyDataset[PartnerExclusionRecord]
        val result = FilterTransform.addExclusionFlag(testData, Some(emptyExclusionList))
        val resultData = result.select("excluded").collect()

        // All should be marked as not excluded
        resultData.foreach(_.getInt(0) shouldBe 0)
      }
    }

    describe("addRestrictedFlag function") {
      it("should add restriction flags based on sensitive advertiser data") {
        val testData = Seq(
          (1, 1001),
          (2, 1002),
          (3, 1003),
          (4, 1004)
        ).toDF("id", "AdvertiserId")

        val sensitiveData = Seq(
          SensitiveAdvertiserRecord("1002", "Health", 1),
          SensitiveAdvertiserRecord("1003", "Normal", 0), // Not restricted
          SensitiveAdvertiserRecord("1004", "HEC", 1)
        ).toDS()

        val result = FilterTransform.addRestrictedFlag(testData, Some(sensitiveData))
        val resultData = result.select("id", "AdvertiserId", "IsRestricted").collect()

        val restrictedMap = resultData.map(row => row.getInt(1) -> row.getInt(2)).toMap
        restrictedMap(1001) shouldBe 0  // Not in sensitive list
        restrictedMap(1002) shouldBe 1  // Restricted
        restrictedMap(1003) shouldBe 0  // In list but not restricted
        restrictedMap(1004) shouldBe 1  // Restricted
      }

      it("should return unchanged data when no sensitive advertiser data provided") {
        val testData = Seq(
          (1, 1001),
          (2, 1002)
        ).toDF("id", "AdvertiserId")

        val result = FilterTransform.addRestrictedFlag(testData, None)
        
        result.columns should contain theSameElementsAs testData.columns
        result.count() shouldBe testData.count()
      }
    }

    describe("filterDataset function") {
      it("should perform inner join when filterAdGroup is true") {
        val baseData = Seq(
          (1, "101", "1001"),
          (2, "102", "1002"),
          (3, "103", "1003"),
          (4, "104", "1004")  // This AdGroup won't be in adgroup dataset
        ).toDF("id", "AdGroupId", "CampaignId")

        val adgroupData = Seq(
          AdGroupRecord("101", "10", "1", "3", "1001"),
          AdGroupRecord("102", "20", "2", "4", "1002"),
          AdGroupRecord("103", "30", "3", "5", "1003")
          // AdGroupId 104 is missing - should be filtered out
        ).toDS()

        val result = FilterTransform.filterDataset(baseData, adgroupData, filterAdGroup = true, None)
        val resultData = result.collect()

        // Should only contain records with matching AdGroupIds (inner join)
        resultData should have length 3
        resultData.map(_.getString(result.columns.indexOf("AdGroupId"))) should contain allOf("101", "102", "103")
        resultData.map(_.getString(result.columns.indexOf("AdGroupId"))) should not contain "104"
      }

      it("should perform left outer join when filterAdGroup is false") {
        val baseData = Seq(
          (1, "101", "1001"),
          (2, "104", "1004")  // This AdGroup won't be in adgroup dataset
        ).toDF("id", "AdGroupId", "CampaignId")

        val adgroupData = Seq(
          AdGroupRecord("101", "10", "1", "3", "1001")
          // AdGroupId 104 is missing
        ).toDS()

        val result = FilterTransform.filterDataset(baseData, adgroupData, filterAdGroup = false, None)
        val resultData = result.collect()

        // Should contain all records (left outer join)
        resultData should have length 2
        resultData.map(_.getString(result.columns.indexOf("AdGroupId"))) should contain allOf("101", "104")
      }

      it("should apply country filter when provided") {
        val baseData = Seq(
          (1, "101", "1001", "US"),
          (2, "102", "1002", "CA"),
          (3, "103", "1003", "GB"),
          (4, "104", "1004", "FR")
        ).toDF("id", "AdGroupId", "CampaignId", "Country")

        val adgroupData = spark.emptyDataset[AdGroupRecord]
        val countryFilter = Seq(
          CountryFilterRecord("US"),
          CountryFilterRecord("CA")
        ).toDS()

        val result = FilterTransform.filterDataset(baseData, adgroupData, filterAdGroup = false, Some(countryFilter))
        val resultData = result.collect()

        // Should only contain records with countries in filter
        resultData should have length 2
        resultData.map(_.getString(result.columns.indexOf("Country"))) should contain allOf("US", "CA")
        resultData.map(_.getString(result.columns.indexOf("Country"))) should not contain("GB")
        resultData.map(_.getString(result.columns.indexOf("Country"))) should not contain("FR")
      }
    }

    describe("matchLandingPage function") {
      it("should join creative landing page data using hashed creative IDs") {
        val testData = Seq(
          (1, "creative_001"),
          (2, "creative_002"),
          (3, "creative_003")
        ).toDF("id", "CreativeId")

        val landingPageData = Seq(
          CreativeLandingPageRecord("creative_001", "landing_001"),
          CreativeLandingPageRecord("creative_002", "landing_002")
          // creative_003 missing - should result in inner join behavior
        ).toDS()

        val result = FilterTransform.matchLandingPage(testData, landingPageData)
        val resultData = result.collect()

        // Should only contain records with matching creative IDs
        resultData should have length 2
        
        val resultCreativeIds = resultData.map(_.getString(1))
        resultCreativeIds should contain allOf("creative_001", "creative_002")
        resultCreativeIds should not contain "creative_003"
        
        // Should contain landing page IDs
        result.columns should contain("CreativeLandingPageId")
      }

      it("should handle empty landing page dataset") {
        val testData = Seq(
          (1, "creative_001"),
          (2, "creative_002")
        ).toDF("id", "CreativeId")

        val emptyLandingPageData = spark.emptyDataset[CreativeLandingPageRecord]
        val result = FilterTransform.matchLandingPage(testData, emptyLandingPageData)

        // Should result in empty dataset due to inner join
        result.count() shouldBe 0
      }
    }

    describe("preFilterJoin function") {
      it("should join clicks with bids/impressions and create labels") {
        val clickData = Seq(
          ("req_001", 1),
          ("req_003", 1)
        ).toDF("BidRequestId", "label")

        val bidsImpsData = Seq(
          ("req_001", 100),
          ("req_002", 200),
          ("req_003", 300),
          ("req_004", 400)
        ).toDF("BidRequestId", "AdWidthInPixels")
        .withColumn("AdHeightInPixels", lit(250))

        val result = FilterTransform.preFilterJoin(clickData.as("clicks"), bidsImpsData.as("bids"))
        val resultData = result.collect()

        // Should contain all bids/impressions records
        resultData should have length 4

        // Check label assignment
        val labelMap = resultData.map(row => 
          row.getString(result.columns.indexOf("BidRequestId")) -> 
          row.getInt(result.columns.indexOf("label"))
        ).toMap
        labelMap("req_001") shouldBe 1  // Had click
        labelMap("req_002") shouldBe 0  // No click
        labelMap("req_003") shouldBe 1  // Had click
        labelMap("req_004") shouldBe 0  // No click

        // Should create AdFormat column
        result.columns should contain("AdFormat")
        resultData.foreach { row =>
          val adFormat = row.getString(result.columns.indexOf("AdFormat"))
          adFormat should fullyMatch regex """\d+x\d+"""
        }
      }
    }
  }

  override def afterAll(): Unit = {
    spark.stop()
  }
}