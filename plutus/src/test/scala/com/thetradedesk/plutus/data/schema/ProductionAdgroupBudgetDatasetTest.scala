package com.thetradedesk.plutus.data.plutus.transform

import com.thetradedesk.TestUtils.TTDSparkTest
import com.thetradedesk.plutus.data.generateDataPathsHourly
import com.thetradedesk.plutus.data.schema.ProductionAdgroupBudgetDataset

import java.time.LocalDateTime
import java.time.format.DateTimeFormatter
import org.scalatest.matchers.should.Matchers._

class ProductionAdgroupBudgetDatasetTest extends TTDSparkTest {
  test("Test ProductionAdgroupBudgetDataset path generators") {
    val formatter = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss")
    val dateTime = LocalDateTime.parse("2024-05-01 03:00:00", formatter)
    val expected = Seq(
      "s3://ttd-vertica-backups/ExportProductionAdGroupBudgetSnapshot/VerticaAws/date=20240501/hour=03",
      "s3://ttd-vertica-backups/ExportProductionAdGroupBudgetSnapshot/VerticaAws/date=20240501/hour=02",
      "s3://ttd-vertica-backups/ExportProductionAdGroupBudgetSnapshot/VerticaAws/date=20240501/hour=01",
      "s3://ttd-vertica-backups/ExportProductionAdGroupBudgetSnapshot/VerticaAws/date=20240501/hour=00",
      "s3://ttd-vertica-backups/ExportProductionAdGroupBudgetSnapshot/VerticaAws/date=20240430/hour=23"
    )
    val result = generateDataPathsHourly(
      ProductionAdgroupBudgetDataset.S3PATH,
      ProductionAdgroupBudgetDataset.S3PATH_GEN,
      dateTime,
      Some(4))

    expected should contain theSameElementsAs result
  }
}
