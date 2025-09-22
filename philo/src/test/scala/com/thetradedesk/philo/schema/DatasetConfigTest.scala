package com.thetradedesk.philo.schema

import com.thetradedesk.philo.config.DatasetParams
import com.thetradedesk.philo.dataset.factory.{GlobalDatasetFactory, HECDatasetFactory, HealthDatasetFactory, AllDatasetFactory}
import org.scalatest.funspec.AnyFunSpec
import org.scalatest.matchers.should.Matchers
import org.apache.spark.sql.functions.{col, lit}
import java.time.LocalDate

class DatasetConfigTest extends AnyFunSpec with Matchers {
  
  val testDate = LocalDate.of(2024, 1, 15)
  val testOutputPath = "s3://test-bucket/data"
  val testTtdEnv = "dev"
  val testExperimentName = "test-experiment"
  val testParams = DatasetParams(testOutputPath, testTtdEnv, testDate)
  val testParamsWithExperiment = DatasetParams(testOutputPath, testTtdEnv, testDate, testExperimentName)
  
  describe("DatasetConfig Factories") {
    
    describe("GlobalDatasetFactory") {
      describe("data configuration") {
        val config = GlobalDatasetFactory.data(testParams)
        
        it("should generate correct write path") {
          config.getWritePath shouldBe "s3://test-bucket/data/dev/global/year=2024/month=01/day=15"
        }
        

        it("should have correct filter condition") {
          config.filterCondition.toString should include("CategoryPolicy")
          config.filterCondition.toString should include("excluded")
        }
      }
      
      describe("metadata configuration") {
        val config = GlobalDatasetFactory.metadata(testParams)
        
        it("should generate correct metadata write path") {
          config.getWritePath shouldBe "s3://test-bucket/data/dev/globalmetadata/year=2024/month=01/day=15"
        }

      }
      
      describe("line counter configuration") {
        val config = GlobalDatasetFactory.lineCounter(testParams)
        
        it("should generate correct line count write path") {
          config.getWritePath shouldBe "s3://test-bucket/data/dev/globallinecounts/year=2024/month=01/day=15"
        }
      }
    }
    
    describe("HECDatasetFactory") {
      describe("data configuration") {
        val config = HECDatasetFactory.data(testParams)
        
        it("should generate correct write path") {
          config.getWritePath shouldBe "s3://test-bucket/data/dev/HEC/year=2024/month=01/day=15"
        }
        

        it("should have CategoryPolicy = HEC filter") {
          config.filterCondition.toString should include("CategoryPolicy")
          config.filterCondition.toString should include("HEC")
        }
      }
      
      describe("excluded metadata configuration") {
        val config = HECDatasetFactory.excludedMetadata(testParams)
        
        it("should properly generate excluded metadata path") {
          config.getWritePath shouldBe "s3://test-bucket/data/dev/HECexcludedmetadata/year=2024/month=01/day=15"
        }
      }
    }
    
    describe("experiment path generation") {
      val dataConfig = GlobalDatasetFactory.data(testParamsWithExperiment)
      val metadataConfig = GlobalDatasetFactory.metadata(testParamsWithExperiment)
      
      it("should generate correct write path with experiment") {
        dataConfig.getWritePath shouldBe "s3://test-bucket/data/dev/experiment=test-experiment/global/year=2024/month=01/day=15"
      }
      
      it("should generate correct metadata write path with experiment") {
        metadataConfig.getWritePath shouldBe "s3://test-bucket/data/dev/experiment=test-experiment/globalmetadata/year=2024/month=01/day=15"
      }
    }
    
    describe("excluded dataset path generation") {
      val excludedConfig = GlobalDatasetFactory.excluded(testParams)
      val excludedMetadataConfig = GlobalDatasetFactory.excludedMetadata(testParams)
      val excludedLineCountConfig = GlobalDatasetFactory.excludedLineCounter(testParams)
      
      it("should add 'excluded' to write path") {
        excludedConfig.getWritePath shouldBe "s3://test-bucket/data/dev/globalexcluded/year=2024/month=01/day=15"
      }
      
      it("should add 'excluded' to metadata path") {
        excludedMetadataConfig.getWritePath shouldBe "s3://test-bucket/data/dev/globalexcludedmetadata/year=2024/month=01/day=15"
      }
      
      it("should add 'excluded' to line count path") {
        excludedLineCountConfig.getWritePath shouldBe "s3://test-bucket/data/dev/globalexcludedlinecounts/year=2024/month=01/day=15"
      }
    }
    
    describe("HealthDatasetFactory") {
      val config = HealthDatasetFactory.data(testParams)
      
      it("should filter for CategoryPolicy = Health") {
        config.filterCondition.toString should include("CategoryPolicy")
        config.filterCondition.toString should include("Health")
      }
    }
    
    describe("AllDatasetFactory") {
      val config = AllDatasetFactory.data(testParams)

      it("should have no business logic filter (lit(true))") {
        config.filterCondition.toString should include("true")
      }
    }
    
    describe("edge cases") {
      it("should handle different date formats") {
        val customParams = DatasetParams(testOutputPath, testTtdEnv, LocalDate.of(2024, 12, 5))
        val config = GlobalDatasetFactory.data(customParams)
        config.getWritePath should include("year=2024/month=12/day=05")
      }
    }
  }
}