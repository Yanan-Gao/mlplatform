package com.thetradedesk.featurestore.jobs

import com.thetradedesk.featurestore.testutils.TTDSparkTest
import org.scalatest.matchers.should.Matchers
import com.thetradedesk.spark.TTDSparkContext.spark.implicits._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.Row


class DailySeedMappingIdDensityScoreTest extends TTDSparkTest with Matchers {

  test("only active mapping ids should be kept") {
    val seedDensity = Seq(
      ("AliasedSupplyPublisherIdCity", 1L, Array(100, 200), Array(300, 400)),
      ("SiteZip", 2L, Array(100, 200), Array(300, 400)),
    ).toDF("FeatureKey", "FeatureValueHashed", "SyntheticIdLevel1", "SyntheticIdLevel2")

    val campaignSeeds = Seq(
      ("campaignid1", "seedid1"),
      ("campaignid2", "seedid2"),
      ("campaignid3", "seedid3"),
      ("campaignid4", "seedid4"),
    ).toDF("CampaignId", "SeedId")

    val policyTable = Seq(
      (100, 1, "seedid1"),
      (200, 2, "seedid2"),
      (300, 3, "seedid3"),
      (400, 4, "seedid4"),
    ).toDF("SyntheticId", "MappingId", "SeedId")

    val activeCampaigns = Seq(
      ("campaignid1"),
      ("campaignid3"),
    ).toDF("CampaignId")

    val transformed = DailySeedMappingIdDensityScore.transform(
      seedDensity,
      campaignSeeds,
      policyTable,
      activeCampaigns,
      3000
    )

    transformed
      .withColumn("MappingIdLevel1", concat_ws(",", $"MappingIdLevel1"))
      .withColumn("MappingIdLevel1S1", concat_ws(",", $"MappingIdLevel1S1"))
      .withColumn("MappingIdLevel2", concat_ws(",", $"MappingIdLevel2"))
      .withColumn("MappingIdLevel2S1", concat_ws(",", $"MappingIdLevel2S1"))
      //.as[(String, Array[Short], Array[Short], Array[Short], Array[Short])]
      .collect().toList should contain theSameElementsAs(
      Seq(
        Row("1_1", "1", "", "3", ""),
        Row("2_2", "1", "", "3", "")
      )
    )
  }

  test("overflown mapping id should carry to slot 1") {
    val seedDensity = Seq(
      ("AliasedSupplyPublisherIdCity", 1L, Array(100, 200), Array(300, 400)),
      ("SiteZip", 2L, Array(100, 200), Array(300, 400)),
    ).toDF("FeatureKey", "FeatureValueHashed", "SyntheticIdLevel1", "SyntheticIdLevel2")

    val campaignSeeds = Seq(
      ("campaignid1", "seedid1"),
      ("campaignid2", "seedid2"),
      ("campaignid3", "seedid3"),
      ("campaignid4", "seedid4"),
    ).toDF("CampaignId", "SeedId")

    val policyTable = Seq(
      (100, 65537, "seedid1"),
      (200, 2, "seedid2"),
      (300, 65538, "seedid3"),
      (400, 4, "seedid4"),
    ).toDF("SyntheticId", "MappingId", "SeedId")

    val activeCampaigns = Seq(
      ("campaignid1"),
      ("campaignid3"),
    ).toDF("CampaignId")

    val transformed = DailySeedMappingIdDensityScore.transform(
      seedDensity,
      campaignSeeds,
      policyTable,
      activeCampaigns,
      3000
    )

    transformed
      .withColumn("MappingIdLevel1", concat_ws(",", $"MappingIdLevel1"))
      .withColumn("MappingIdLevel1S1", concat_ws(",", $"MappingIdLevel1S1"))
      .withColumn("MappingIdLevel2", concat_ws(",", $"MappingIdLevel2"))
      .withColumn("MappingIdLevel2S1", concat_ws(",", $"MappingIdLevel2S1"))
      .collect().toList should contain theSameElementsAs(
      Seq(
        Row("1_1", "", "1", "", "2"),
        Row("2_2", "", "1", "", "2")
      )
      )
  }

  test("mapping ids should be capped by maxNumMappingIdsInAerospike") {
    val seedDensity = Seq(
      ("AliasedSupplyPublisherIdCity", 1L, Array(100, 200), Array(300, 400)),
      ("SiteZip", 2L, Array(100, 200), Array(300, 400)),
    ).toDF("FeatureKey", "FeatureValueHashed", "SyntheticIdLevel1", "SyntheticIdLevel2")

    val campaignSeeds = Seq(
      ("campaignid1", "seedid1"),
      ("campaignid2", "seedid2"),
      ("campaignid3", "seedid3"),
      ("campaignid4", "seedid4"),
    ).toDF("CampaignId", "SeedId")

    val policyTable = Seq(
      (100, 1, "seedid1"),
      (200, 2, "seedid2"),
      (300, 3, "seedid3"),
      (400, 4, "seedid4"),
    ).toDF("SyntheticId", "MappingId", "SeedId")

    val activeCampaigns = Seq(
      ("campaignid1"),
      ("campaignid2"),
      ("campaignid3"),
      ("campaignid4"),
    ).toDF("CampaignId")

    val transformed = DailySeedMappingIdDensityScore.transform(
      seedDensity,
      campaignSeeds,
      policyTable,
      activeCampaigns,
      2
    )

    transformed
      .withColumn("MappingIdLevel1", concat_ws(",", $"MappingIdLevel1"))
      .withColumn("MappingIdLevel1S1", concat_ws(",", $"MappingIdLevel1S1"))
      .withColumn("MappingIdLevel2", concat_ws(",", $"MappingIdLevel2"))
      .withColumn("MappingIdLevel2S1", concat_ws(",", $"MappingIdLevel2S1"))
      //.as[(String, Array[Short], Array[Short], Array[Short], Array[Short])]
      .collect().toList should contain theSameElementsAs(
      Seq(
        Row("1_1", "1,2", "", "", ""),
        Row("2_2", "1,2", "", "", "")
      )
      )
  }

}
