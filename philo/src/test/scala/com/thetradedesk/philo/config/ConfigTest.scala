package com.thetradedesk.philo.config

import org.scalatest.funspec.AnyFunSpec
import org.scalatest.matchers.should.Matchers
import org.apache.spark.sql.functions.{col, lit}
import java.time.LocalDate

class ConfigTest extends AnyFunSpec with Matchers {



  describe("DatasetConfig") {
    
    it("should generate correct write path") {
      val config = DatasetConfig(
        name = "test-dataset",
        partitions = 1,
        filterCondition = lit(true),
        outputPath = "s3://test-bucket/data",
        writeEnv = "dev",
        date = LocalDate.of(2024, 1, 15),
        experimentName = "test-exp",
        isRestricted = None
      )

      val writePath = config.getWritePath
      writePath shouldBe "s3://test-bucket/data/dev/experiment=test-exp/test-dataset/year=2024/month=01/day=15"
    }

    it("should generate write path without experiment name") {
      val config = DatasetConfig(
        name = "test-dataset",
        partitions = 1,
        filterCondition = lit(true),
        outputPath = "s3://test-bucket/data",
        writeEnv = "prod",
        date = LocalDate.of(2024, 6, 5),
        experimentName = null,
        isRestricted = None
      )

      val writePath = config.getWritePath
      writePath shouldBe "s3://test-bucket/data/prod/test-dataset/year=2024/month=06/day=05"
    }
  }
}