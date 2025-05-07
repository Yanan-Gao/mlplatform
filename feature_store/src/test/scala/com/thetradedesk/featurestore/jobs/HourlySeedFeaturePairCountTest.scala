package com.thetradedesk.featurestore.jobs;

import com.thetradedesk.featurestore.rsm.CommonEnums.DataSource
import com.thetradedesk.featurestore.testutils.TTDSparkTest
import com.thetradedesk.spark.TTDSparkContext.spark.implicits._
import org.apache.spark.sql.Row
import org.scalatest.matchers.should.Matchers

class HourlySeedFeaturePairCountTest extends TTDSparkTest with Matchers {

  test("aggregateFeatureCount should produce correct seed feature count") {
    val bidReq = Seq(
      ("TDID1", 1, 2),
      ("TDID1", 3, 1),
      ("TDID1", 2, 3),
      ("TDID2", 1, 2),
      ("TDID3", 3, 4)
    ).toDF("TDID", "AliasedSupplyPublisherIdCityHashed", "SiteZipHashed")

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
}