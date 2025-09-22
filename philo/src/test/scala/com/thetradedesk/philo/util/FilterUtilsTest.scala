package com.thetradedesk.philo.util

import org.scalatest.funspec.AnyFunSpec
import org.scalatest.matchers.should.Matchers
import org.scalatest.BeforeAndAfterAll
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions.col
import com.thetradedesk.philo.schema.{AdGroupRecord, CountryFilterRecord}
import java.time.LocalDate

class FilterUtilsTest extends AnyFunSpec with Matchers with BeforeAndAfterAll {

  implicit val spark: SparkSession = SparkSession.builder()
    .appName("FilterUtilsTest")
    .master("local[*]")
    .config("spark.sql.adaptive.enabled", "false")
    .config("spark.sql.adaptive.coalescePartitions.enabled", "false")
    .getOrCreate()

  import spark.implicits._

  describe("FilterUtils") {
    
    describe("getAdGroupFilter function") {
      it("should exist and have correct method signature") {
        // This test verifies the function exists with the expected signature
        // Full integration testing would require external S3 datasets (CampaignROIGoalDataSet)
        
        val filterUtilsClass = FilterUtils.getClass
        val methods = filterUtilsClass.getDeclaredMethods
        val getAdGroupFilterMethod = methods.find(_.getName == "getAdGroupFilter")
        
        getAdGroupFilterMethod shouldBe defined
        getAdGroupFilterMethod.get.getParameterCount shouldBe 4 // date, adgroup, roi_types, spark
      }
    }

    describe("getCountryFilter function") {
      it("should read country filter from valid CSV file") {
        // Create a temporary CSV file with country data
        val tempPath = "/tmp/test_countries.csv"
        val countryCsv = "US\nCA\nGB\n"
        
        // Write test data to temporary file
        import java.io.PrintWriter
        val writer = new PrintWriter(tempPath)
        try {
          writer.write(countryCsv)
        } finally {
          writer.close()
        }

        val result = FilterUtils.getCountryFilter(tempPath)
        val resultData = result.collect()

        resultData should have length 3
        resultData.map(_.Country) should contain allOf("US", "CA", "GB")

        // Clean up
        new java.io.File(tempPath).delete()
      }

      it("should handle empty path by throwing exception") {
        // Test with empty string path - function throws exception for empty paths
        val emptyPath = ""
        
        an[Exception] should be thrownBy {
          FilterUtils.getCountryFilter(emptyPath)
        }
      }

      it("should have correct method signature") {
        // Verify the function exists with expected signature
        val filterUtilsClass = FilterUtils.getClass
        val methods = filterUtilsClass.getDeclaredMethods
        val getCountryFilterMethod = methods.find(_.getName == "getCountryFilter")
        
        getCountryFilterMethod shouldBe defined
        getCountryFilterMethod.get.getParameterCount shouldBe 2 // countryFilePath, spark
      }
    }
  }

  override def afterAll(): Unit = {
    spark.stop()
  }
}