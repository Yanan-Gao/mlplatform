package com.thetradedesk.philo.dataset.factory

import org.scalatest.funspec.AnyFunSpec
import org.scalatest.matchers.should.Matchers
import org.apache.spark.sql.functions.{col, lit}
import org.apache.spark.sql.{DataFrame, SparkSession}
import com.thetradedesk.philo.config.DatasetParams
import java.time.LocalDate

class DatasetFactoryTest extends AnyFunSpec with Matchers {

  // Test Spark session
  implicit val spark: SparkSession = SparkSession.builder()
    .appName("DatasetFactoryTest")
    .master("local[2]")
    .config("spark.sql.adaptive.enabled", "false")
    .config("spark.sql.adaptive.coalescePartitions.enabled", "false")
    .getOrCreate()

  import spark.implicits._

  val testParams = DatasetParams(
    outputPath = "s3://test-bucket/data",
    writeEnv = "dev", 
    date = LocalDate.of(2024, 1, 15),
    experimentName = "test-experiment"
  )

  describe("DatasetFactory trait") {
    
    it("should generate consistent metadata names") {
      val factories = List(GlobalDatasetFactory, AllDatasetFactory, HECDatasetFactory, HealthDatasetFactory)
      
      factories.foreach { factory =>
        val metadataConfig = factory.metadata(testParams)
        
        metadataConfig.name shouldBe factory.metadataName
      }
    }

    it("should generate consistent excluded names") {
      val factories = List(GlobalDatasetFactory, AllDatasetFactory, HECDatasetFactory, HealthDatasetFactory)
      
      factories.foreach { factory =>
        val dataConfig = factory.data(testParams)
        val excludedConfig = factory.excluded(testParams)
        
        excludedConfig.name shouldBe s"${dataConfig.name}excluded"
      }
    }

    it("should generate consistent line counter names") {
      val factories = List(GlobalDatasetFactory, AllDatasetFactory, HECDatasetFactory, HealthDatasetFactory)
      
      factories.foreach { factory =>
        val lineCountConfig = factory.lineCounter(testParams)
        
        lineCountConfig.name shouldBe factory.lineCountName
      }
    }
  }

  describe("CategoryPolicy filtering logic") {
    
    // Create test data with different CategoryPolicy values
    def createTestData(): DataFrame = {
      val testData = Seq(
        ("Health", 0, "label1", "id1"),     // Health, not excluded
        ("Health", 1, "label2", "id2"),     // Health, excluded  
        ("Health", 0, "label3", "id3"),     // Health, not excluded
        ("HEC", 0, "label4", "id4"),        // HEC, not excluded
        ("HEC", 1, "label5", "id5"),        // HEC, excluded
        ("HEC", 0, "label6", "id6"),        // HEC, not excluded
        (null, 0, "label7", "id7"),         // Global (null CategoryPolicy), not excluded
        (null, 1, "label8", "id8"),         // Global (null CategoryPolicy), excluded
        (null, 0, "label9", "id9")          // Global (null CategoryPolicy), not excluded
      ).toDF("CategoryPolicy", "excluded", "label", "id")
      
      testData
    }

    it("should filter Health data correctly") {
      val testData = createTestData()
      
      // Test Health factory filtering
      val healthConfig = HealthDatasetFactory.data(testParams)
      val healthExcludedConfig = HealthDatasetFactory.excluded(testParams)
      
      // Apply filters and count results
      val healthFiltered = testData.filter(healthConfig.filterCondition)
      val healthExcludedFiltered = testData.filter(healthExcludedConfig.filterCondition)
      
      // Validate Health data (CategoryPolicy = "Health" AND excluded = 0)
      val healthRows = healthFiltered.collect()
      healthRows should have length 2
      healthRows.foreach { row =>
        row.getAs[String]("CategoryPolicy") shouldBe "Health"
        row.getAs[Int]("excluded") shouldBe 0
      }
      
      // Validate Health excluded data (CategoryPolicy = "Health" AND excluded = 1)
      val healthExcludedRows = healthExcludedFiltered.collect()
      healthExcludedRows should have length 1
      healthExcludedRows.foreach { row =>
        row.getAs[String]("CategoryPolicy") shouldBe "Health"
        row.getAs[Int]("excluded") shouldBe 1
      }
    }

    it("should filter HEC data correctly") {
      val testData = createTestData()
      
      // Test HEC factory filtering
      val hecConfig = HECDatasetFactory.data(testParams)
      val hecExcludedConfig = HECDatasetFactory.excluded(testParams)
      
      // Apply filters and count results
      val hecFiltered = testData.filter(hecConfig.filterCondition)
      val hecExcludedFiltered = testData.filter(hecExcludedConfig.filterCondition)
      
      // Validate HEC data (CategoryPolicy = "HEC" AND excluded = 0)
      val hecRows = hecFiltered.collect()
      hecRows should have length 2
      hecRows.foreach { row =>
        row.getAs[String]("CategoryPolicy") shouldBe "HEC"
        row.getAs[Int]("excluded") shouldBe 0
      }
      
      // Validate HEC excluded data (CategoryPolicy = "HEC" AND excluded = 1)
      val hecExcludedRows = hecExcludedFiltered.collect()
      hecExcludedRows should have length 1
      hecExcludedRows.foreach { row =>
        row.getAs[String]("CategoryPolicy") shouldBe "HEC"
        row.getAs[Int]("excluded") shouldBe 1
      }
    }

    it("should filter Global data correctly") {
      val testData = createTestData()
      
      // Test Global factory filtering  
      val globalConfig = GlobalDatasetFactory.data(testParams)
      val globalExcludedConfig = GlobalDatasetFactory.excluded(testParams)
      
      // Apply filters and count results
      val globalFiltered = testData.filter(globalConfig.filterCondition)
      val globalExcludedFiltered = testData.filter(globalExcludedConfig.filterCondition)
      
      // Validate Global data (CategoryPolicy IS NULL AND excluded = 0)
      val globalRows = globalFiltered.collect()
      globalRows should have length 2
      globalRows.foreach { row =>
        Option(row.getAs[String]("CategoryPolicy")) shouldBe None // null check
        row.getAs[Int]("excluded") shouldBe 0
      }
      
      // Validate Global excluded data (CategoryPolicy IS NULL AND excluded = 1)
      val globalExcludedRows = globalExcludedFiltered.collect()
      globalExcludedRows should have length 1
      globalExcludedRows.foreach { row =>
        Option(row.getAs[String]("CategoryPolicy")) shouldBe None // null check
        row.getAs[Int]("excluded") shouldBe 1
      }
    }

    it("should ensure data separation between factories") {
      val testData = createTestData()
      
      // Get all filter conditions
      val healthFilter = HealthDatasetFactory.data(testParams).filterCondition
      val hecFilter = HECDatasetFactory.data(testParams).filterCondition  
      val globalFilter = GlobalDatasetFactory.data(testParams).filterCondition
      
      // Apply filters
      val healthData = testData.filter(healthFilter)
      val hecData = testData.filter(hecFilter)
      val globalData = testData.filter(globalFilter)
      
      // Verify counts (no overlap)
      val healthCount = healthData.count()
      val hecCount = hecData.count()
      val globalCount = globalData.count()
      val totalNonExcluded = testData.filter(col("excluded") === 0).count()
      
      // Each factory should get its own subset
      healthCount shouldBe 2   // 2 Health records with excluded=0
      hecCount shouldBe 2      // 2 HEC records with excluded=0  
      globalCount shouldBe 2   // 2 Global (null) records with excluded=0
      
      // Total should add up
      (healthCount + hecCount + globalCount) shouldBe totalNonExcluded
      
      // Verify no data overlap by checking distinct categories
      val healthCategories = healthData.select("CategoryPolicy").distinct().collect()
      val hecCategories = hecData.select("CategoryPolicy").distinct().collect()
      val globalCategories = globalData.select("CategoryPolicy").distinct().collect()
      
      // Health should only have "Health"
      healthCategories should have length 1
      healthCategories(0).getAs[String]("CategoryPolicy") shouldBe "Health"
      
      // HEC should only have "HEC"
      hecCategories should have length 1
      hecCategories(0).getAs[String]("CategoryPolicy") shouldBe "HEC"
      
      // Global should only have null
      globalCategories should have length 1
      Option(globalCategories(0).getAs[String]("CategoryPolicy")) shouldBe None
    }
  }

  describe("Partition configuration") {
    
    it("should apply correct partition counts") {
      // Test that each factory returns the expected partition counts
      val healthConfig = HealthDatasetFactory.data(testParams)  
      val hecConfig = HECDatasetFactory.data(testParams)
      val globalConfig = GlobalDatasetFactory.data(testParams)
      
      // Check partition counts match factory definitions
      healthConfig.partitions shouldBe HealthDatasetFactory.dataPartitions
      hecConfig.partitions shouldBe HECDatasetFactory.dataPartitions  
      globalConfig.partitions shouldBe GlobalDatasetFactory.dataPartitions
      
      // Metadata should always be 1 partition
      val healthMetadata = HealthDatasetFactory.metadata(testParams)
      val hecMetadata = HECDatasetFactory.metadata(testParams)
      val globalMetadata = GlobalDatasetFactory.metadata(testParams)
      
      healthMetadata.partitions shouldBe 1
      hecMetadata.partitions shouldBe 1
      globalMetadata.partitions shouldBe 1
    }
    
    it("should handle excluded partitions correctly") {
      val healthExcluded = HealthDatasetFactory.excluded(testParams)
      val hecExcluded = HECDatasetFactory.excluded(testParams)
      val globalExcluded = GlobalDatasetFactory.excluded(testParams)
      
      healthExcluded.partitions shouldBe HealthDatasetFactory.excludedPartitions
      hecExcluded.partitions shouldBe HECDatasetFactory.excludedPartitions
      globalExcluded.partitions shouldBe GlobalDatasetFactory.excludedPartitions
    }
  }

  describe("DatasetConfig generation") {
    
    it("should generate correct S3 paths") {
      val healthConfig = HealthDatasetFactory.data(testParams)
      val expectedPath = s"${testParams.outputPath}/${testParams.writeEnv}/experiment=${testParams.experimentName}/Health/year=2024/month=01/day=15"
      
      healthConfig.getWritePath shouldBe expectedPath
    }
    
    it("should handle isRestricted flag correctly") {
      val healthConfig = HealthDatasetFactory.data(testParams)  
      val globalConfig = GlobalDatasetFactory.data(testParams)
      
      // Health and HEC should have isRestricted = Some(1)
      healthConfig.isRestricted shouldBe Some(1)
      
      // Global should have isRestricted = Some(0)  
      globalConfig.isRestricted shouldBe Some(0)
    }
  }
}