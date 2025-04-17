package com.thetradedesk.plutus.data.schema


import com.thetradedesk.plutus.data.schema.campaignbackoff._
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers

import java.time.{LocalDate, LocalDateTime}

class DatasetPathGenTest extends AnyFunSuite with Matchers {
  test("CampaignAdjustmentsDataset should generate the right path") {
    val path = CampaignAdjustmentsDataset.genPathForDate(LocalDate.of(2024, 9, 22), "prod")
    path shouldEqual "s3://thetradedesk-mlplatform-us-east-1/env=prod/data/plutusbackoff/campaignadjustments/v=1/date=20240922"
  }

  test("CampaignFlightDataset should generate the right path") {
    val path = CampaignFlightDataset.genPathForDate(LocalDate.of(2024, 7, 3), "prod")
    path shouldEqual "s3://thetradedesk-useast-qubole/warehouse.external/thetradedesk.db/provisioning/campaignflight/v=1/date=20240703"
  }

  test("CampaignThrottleMetricDataset should generate the right path") {
    val path = CampaignThrottleMetricDataset.genPathForDate(LocalDate.of(2024, 7, 3), "prod")
    path shouldEqual "s3://thetradedesk-mlplatform-us-east-1/model_monitor/mission_control/env=prod/aggregate-pacing-statistics/v=2/metric=throttle_metric_campaign_parquet/date=20240703"
  }

  test("HadesCampaignAdjustmentsDataset should generate the right path") {
    val path = HadesCampaignAdjustmentsDataset.genPathForDate(LocalDate.of(2025, 11, 13), "prod")
    path shouldEqual "s3://thetradedesk-mlplatform-us-east-1/env=prod/data/plutusbackoff/hadesadjustments/v=2/date=20251113"
  }

  test("MergedCampaignAdjustmentsDataset should generate the right path") {
    val path = MergedCampaignAdjustmentsDataset.genPathForDate(LocalDate.of(2024, 9, 22), "prod")
    path shouldEqual "s3://thetradedesk-mlplatform-us-east-1/env=prod/data/plutusbackoff/campaignadjustmentspacing/v=1/date=20240922"
  }

  test("PlatformWideStatsDataset should generate the right path") {
    val path = PlatformWideStatsDataset.genPathForDate(LocalDate.of(2024, 9, 22), "prod")
    path shouldEqual "s3://thetradedesk-mlplatform-us-east-1/env=prod/data/plutusbackoff/platformwidestats/v=1/date=20240922"
  }

  test("PlutusCampaignAdjustmentsDataset should generate the right path") {
    val path = PlutusCampaignAdjustmentsDataset.genPathForDate(LocalDate.of(2025, 11, 13), "prod")
    path shouldEqual "s3://thetradedesk-mlplatform-us-east-1/env=prod/data/plutusbackoff/plutusadjustments/v=1/date=20251113"
  }


  test("PcResultsMergedDataset should generate the right path") {
    var path = PcResultsMergedDataset.genPathForDate(LocalDate.of(2025, 1, 25), "prod")
    path shouldEqual "s3://ttd-identity/datapipeline/prod/pcresultsgeronimo/v=3/date=20250125"

    // Testing path with hour in [0,9]
    path = PcResultsMergedDataset.generatePathForHour(LocalDateTime.of(2024, 12, 8, 1, 0), "prod")
    path shouldEqual "s3://ttd-identity/datapipeline/prod/pcresultsgeronimo/v=3/date=20241208/hour=1"

    // testing path with hour in [10,12]
    path = PcResultsMergedDataset.generatePathForHour(LocalDateTime.of(2024, 7, 3, 12, 0), "prod")
    path shouldEqual "s3://ttd-identity/datapipeline/prod/pcresultsgeronimo/v=3/date=20240703/hour=12"
  }

  test("PlutusOptoutBidsDataset should generate the right path") {
    var path = PlutusOptoutBidsDataset.genPathForDate(LocalDate.of(2025, 1, 26), "prod")
    path shouldEqual "s3://ttd-identity/datapipeline/prod/pc_optout_bids/v=1/date=20250126"

    path = PlutusOptoutBidsDataset.generatePathForHour(LocalDateTime.of(2025, 1, 26, 1, 0), "prod")
    path shouldEqual "s3://ttd-identity/datapipeline/prod/pc_optout_bids/v=1/date=20250126/hour=1"
  }

  test("ManualCampaignFloorBufferDataset should generate the right path") {
    val path = CampaignFloorBufferDataset.genPathForDate(LocalDate.of(2025, 4, 3), "prod")
    path shouldEqual "s3://thetradedesk-mlplatform-us-east-1/env=prod/data/plutusbackoff/campaignfloorbuffer/v=1/date=20250403"
  }
}
