package com.thetradedesk.featurestore.jobs;

import com.thetradedesk.featurestore.rsm.CommonEnums.DataSource
import com.thetradedesk.featurestore.testutils.TTDSparkTest
import com.thetradedesk.spark.TTDSparkContext.spark.implicits._
import org.apache.spark.sql.Row
import org.scalatest.matchers.should.Matchers

class HourlySeedFeaturePairCountTest extends TTDSparkTest with Matchers {

  test("aggregateFeatureCount should produce correct seed feature count") {
    val bidReq = Seq(
      ("Br1", "TDID1", 1, 2),
      ("Br2", "TDID1", 3, 1),
      ("Br3", "TDID1", 2, 3),
      ("Br4", "TDID2", 1, 2),
      ("Br5", "TDID3", 3, 4)
    ).toDF("BidRequestId", "TDID", "AliasedSupplyPublisherIdCityHashed", "SiteZipHashed")

    val aggregatedSeed = Seq(
      ("TDID1", Seq("seed1", "seed2", "ttdown1")),
      ("TDID2", Seq("seed1", "seed3", "ttdown1"))
    ).toDF("TDID", "SeedIds")

    val policyTable = Seq(
      ("seed1", DataSource.Seed.id, false),
      ("seed2", DataSource.Seed.id, true),
      ("seed3", DataSource.Seed.id, false),
      ("ttdown1", DataSource.TTDOwnData.id, false),
    ).toDF("SeedId", "Source", "IsSensitive")


    val res = HourlySeedFeaturePairCount.aggregateFeatureCount(
        bidReq, aggregatedSeed, policyTable
      )
      .orderBy("SeedId", "FeatureKey", "FeatureValueHashed")

    res
      .select("SeedId", "FeatureKey", "FeatureValueHashed", "HourlyCount") // re-order for readability
      .collect().toList should contain theSameElementsAs (
      Seq(
        Row("seed1", "SiteZip", 1, 1),
        Row("seed1", "SiteZip", 2, 2),
        Row("seed1", "SiteZip", 3, 1),
        Row("seed2", "AliasedSupplyPublisherIdCity", 1, 1),
        Row("seed2", "AliasedSupplyPublisherIdCity", 2, 1),
        Row("seed2", "AliasedSupplyPublisherIdCity", 3, 1),
        Row("seed3", "SiteZip", 2, 1),
        Row("ttdown1", "AliasedSupplyPublisherIdCity", 1, 2),
        Row("ttdown1", "AliasedSupplyPublisherIdCity", 2, 1),
        Row("ttdown1", "AliasedSupplyPublisherIdCity", 3, 1),
        Row("ttdown1", "SiteZip", 1, 1),
        Row("ttdown1", "SiteZip", 2, 2),
        Row("ttdown1", "SiteZip", 3, 1),
      )
      )
  }

  test("aggregateFeatureCount should produce correct seed feature count with multiple IDs") {
    val bidReq = Seq(
      ("Br1", "TDID1", 1, 2),
      ("Br1", "TDID2", 1, 2),
      ("Br3", "TDID1", 2, 3),
      ("Br3", "TDID2", 2, 3),
      ("Br5", "TDID3", 3, 4)
    ).toDF("BidRequestId", "TDID", "AliasedSupplyPublisherIdCityHashed", "SiteZipHashed")

    val aggregatedSeed = Seq(
      ("TDID1", Seq("seed1", "seed2", "ttdown1")),
      ("TDID2", Seq("seed1", "seed3", "ttdown1"))
    ).toDF("TDID", "SeedIds")

    val policyTable = Seq(
      ("seed1", DataSource.Seed.id, false),
      ("seed2", DataSource.Seed.id, true),
      ("seed3", DataSource.Seed.id, false),
      ("ttdown1", DataSource.TTDOwnData.id, false),
    ).toDF("SeedId", "Source", "IsSensitive")


    val res = HourlySeedFeaturePairCount.aggregateFeatureCount(
        bidReq, aggregatedSeed, policyTable
      )
      .orderBy("SeedId", "FeatureKey", "FeatureValueHashed")

    res
      .select("SeedId", "FeatureKey", "FeatureValueHashed", "HourlyCount") // re-order for readability
      .collect().toList should contain theSameElementsAs (
      Seq(
        Row("seed1", "SiteZip", 2, 1),
        Row("seed1", "SiteZip", 3, 1),
        Row("seed2", "AliasedSupplyPublisherIdCity", 1, 1),
        Row("seed2", "AliasedSupplyPublisherIdCity", 2, 1),
        Row("seed3", "SiteZip", 2, 1),
        Row("seed3", "SiteZip", 3, 1),
        Row("ttdown1", "AliasedSupplyPublisherIdCity", 1, 1),
        Row("ttdown1", "AliasedSupplyPublisherIdCity", 2, 1),
        Row("ttdown1", "SiteZip", 2, 1),
        Row("ttdown1", "SiteZip", 3, 1),
      )
      )
  }

  test("aggregateFeatureCount should produce correct seed feature count with multiple IDs with multi count") {
    val bidReq = Seq(
      ("Br1", "TDID1", 1, 2),
      ("Br1", "UID2", 1, 2),
      ("Br2", "TDID1", 1, 2),
      ("Br3", "UID2", 1, 2)
    ).toDF("BidRequestId", "TDID", "AliasedSupplyPublisherIdCityHashed", "SiteZipHashed")

    val aggregatedSeed = Seq(
      ("TDID1", Seq("seed1", "seed2", "ttdown1")),
      ("UID2", Seq("seed1", "seed3", "ttdown1"))
    ).toDF("TDID", "SeedIds")

    val policyTable = Seq(
      ("seed1", DataSource.Seed.id, false),
      ("seed2", DataSource.Seed.id, true),
      ("seed3", DataSource.Seed.id, false),
      ("ttdown1", DataSource.TTDOwnData.id, false),
    ).toDF("SeedId", "Source", "IsSensitive")


    val res = HourlySeedFeaturePairCount.aggregateFeatureCount(
        bidReq, aggregatedSeed, policyTable
      )
      .orderBy("SeedId", "FeatureKey", "FeatureValueHashed")

    res
      .select("SeedId", "FeatureKey", "FeatureValueHashed", "HourlyCount") // re-order for readability
      .collect().toList should contain theSameElementsAs (
      Seq(
        Row("seed1", "SiteZip", 2, 3),
        Row("seed2", "AliasedSupplyPublisherIdCity", 1, 2),
        Row("seed3", "SiteZip", 2, 2),
        Row("ttdown1", "AliasedSupplyPublisherIdCity", 1, 3),
        Row("ttdown1", "SiteZip", 2, 3)
      )
      )
  }

  test("aggregateFeatureCount should return empty when no bid requests") {
    val bidReq = Seq.empty[(String, String, Int, Int)]
      .toDF("BidRequestId", "TDID", "AliasedSupplyPublisherIdCityHashed", "SiteZipHashed")

    val aggregatedSeed = Seq(
      ("TDID1", Seq("seed1")),
    ).toDF("TDID", "SeedIds")

    val policyTable = Seq(
      ("seed1", DataSource.Seed.id, false)
    ).toDF("SeedId", "Source", "IsSensitive")

    val res = HourlySeedFeaturePairCount.aggregateFeatureCount(
      bidReq, aggregatedSeed, policyTable
    )

    res.collect() shouldBe empty
  }

  test("aggregateFeatureCount should handle no matching seeds in aggregatedSeed") {
    val bidReq = Seq(
      ("Br1", "TDIDX", 1, 2),
      ("Br2", "TDIDY", 3, 4),
    ).toDF("BidRequestId", "TDID", "AliasedSupplyPublisherIdCityHashed", "SiteZipHashed")

    val aggregatedSeed = Seq(
      ("TDID1", Seq("seed1")),
      ("TDID2", Seq("seed2"))
    ).toDF("TDID", "SeedIds")

    val policyTable = Seq(
      ("seed1", DataSource.Seed.id, false),
      ("seed2", DataSource.Seed.id, false)
    ).toDF("SeedId", "Source", "IsSensitive")

    val res = HourlySeedFeaturePairCount.aggregateFeatureCount(
      bidReq, aggregatedSeed, policyTable
    )

    res.collect() shouldBe empty
  }

  test("aggregateFeatureCount should aggregate correctly for single ttdown seg with repeated features") {
    val bidReq = Seq(
      ("Br1", "TDID1", 5, 9),
      ("Br2", "TDID1", 5, 9),
      ("Br3", "TDID1", 5, 10),
      ("Br4", "TDID1", 6, 9)
    ).toDF("BidRequestId", "TDID", "AliasedSupplyPublisherIdCityHashed", "SiteZipHashed")

    val aggregatedSeed = Seq(
      ("TDID1", Seq("TTDOWNX"))
    ).toDF("TDID", "SeedIds")

    val policyTable = Seq(
      ("TTDOWNX", DataSource.TTDOwnData.id, false)
    ).toDF("SeedId", "Source", "IsSensitive")

    val res = HourlySeedFeaturePairCount.aggregateFeatureCount(
        bidReq, aggregatedSeed, policyTable
      )
      .orderBy("FeatureKey", "FeatureValueHashed")

    res.select("SeedId", "FeatureKey", "FeatureValueHashed", "HourlyCount")
      .collect().toList should contain theSameElementsAs Seq(
      Row("TTDOWNX", "AliasedSupplyPublisherIdCity", 5, 3), // Br1, Br2, Br3
      Row("TTDOWNX", "AliasedSupplyPublisherIdCity", 6, 1), // Br4
      Row("TTDOWNX", "SiteZip", 9, 3), // Br1, Br2, Br4
      Row("TTDOWNX", "SiteZip", 10, 1) // Br3
    )
  }
}