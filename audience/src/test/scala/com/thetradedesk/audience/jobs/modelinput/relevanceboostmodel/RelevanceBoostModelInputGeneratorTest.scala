package com.thetradedesk.audience.jobs.modelinput.relevanceboostmodel

import com.thetradedesk.audience.utils.TTDSparkTest
import org.scalatest.matchers.should.Matchers
import com.thetradedesk.spark.TTDSparkContext.spark.implicits._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.Row
import org.scalatest.matchers.must.Matchers.contain
import org.scalatest.matchers.should.Matchers.convertToAnyShouldWrapper


class RelevanceBoostModelInputGeneratorTest extends TTDSparkTest {
  test("transform should generate the model input") {
    val policyTableDf = Seq(
      (0, 3, "SeedId1", 10000, false, true)
    ).toDF("CrossDeviceVendorId", "Source", "SourceId", "SyntheticId", "IsSensitive", "IsActive")

    val oosPredictionDf = Seq(
      ("BidRequestId1", Seq(1), Seq(0.9), Seq(0.8), Seq(10000), Seq(2L))
    ).toDF("BidRequestId", "Targets", "pred", "sen_pred", "SyntheticIds", "ZipSiteLevel_Seed")

    val geronimoDf = Seq(
      ("BidRequestId1", 1, Seq(
        1000L, 1001L, 1002L, 1003L, 1004L, 1005L, 1006L, 1007L, 1008L, 1009L,
        2000L, 2001L, 2002L, 2003L, 2004L, 2005L, 2006L, 2007L, 2008L, 2009L,
      ), Seq(
        1000L, 1001L, 1002L, 1003L, 1004L, 1005L, 1006L, 1007L, 1008L, 1009L,
      ), "AdvertiserId1", "CampaignId1", "AdGroupId1")
    ).toDF("BidRequestId", "IsImp", "UserTargetingDataIds", "AdvertiserFirstPartyDataIds", "AdvertiserId", "CampaignId", "AdGroupId")

    val lalResultsDf = Seq(
      ("SeedId1", 1000L, 0.1, true),
      ("SeedId1", 1001L, 0.2, true),
      ("SeedId1", 1002L, 0.3, true),
      ("SeedId1", 1003L, 0.4, true),
      ("SeedId1", 1004L, 0.5, true),
      ("SeedId1", 1005L, 0.6, true),
      ("SeedId1", 1006L, 0.7, true),
      ("SeedId1", 1007L, 0.8, true),
      ("SeedId1", 1008L, 0.9, true),
      ("SeedId1", 1009L, 1.0, true),

      ("SeedId1", 2000L, 1.1, false),
      ("SeedId1", 2001L, 1.2, false),
      ("SeedId1", 2002L, 1.3, false),
      ("SeedId1", 2003L, 1.4, false),
      ("SeedId1", 2004L, 1.5, false),
      ("SeedId1", 2005L, 1.6, false),
      ("SeedId1", 2006L, 1.7, false),
      ("SeedId1", 2007L, 1.8, false),
      ("SeedId1", 2008L, 1.9, false),
      ("SeedId1", 2009L, 2.0, false),
    ).toDF("SeedId", "TargetingDataId", "RelevanceRatio", "IsFirstPartyData")

    val stringifyMap = udf((m: Map[Long, Double]) => {
      m.toList.sortBy(_._1).mkString(",")
    })

    val input = RelevanceBoostModelInputGeneratorJob.transform(
        policyTableDf,
        oosPredictionDf,
        geronimoDf,
        lalResultsDf
      )
      .withColumn("DataRelevanceMapStr", stringifyMap($"DataRelevanceMap"))
      .withColumn("AdvertiserFirstPartyDataRelevanceMapStr", stringifyMap($"AdvertiserFirstPartyDataRelevanceMap"))
      .select(
        $"BidRequestId",
        $"AdvertiserId",
        $"CampaignId",
        $"AdGroupId",
        $"IsSensitive",
        $"Target",
        $"Original_NeoScore",
        $"SeedId",
        $"ZipSiteLevel_Seed",
        $"DataRelevanceMapStr",
        $"AdvertiserFirstPartyDataRelevanceMapStr",

        $"MatchedFpdRelevanceP50",
        $"MatchedFpdRelevanceP90",
        $"MatchedFpdRelevanceSum",
        $"MatchedFpdRelevanceStddev",
        $"MatchedFpdCount",

        $"MatchedTpdRelevanceP50",
        $"MatchedTpdRelevanceP90",
        $"MatchedTpdRelevanceSum",
        $"MatchedTpdRelevanceStddev",
        $"MatchedTpdCount",

        $"MatchedFpdRelevanceP90xZipSiteLevel",
        $"MatchedTpdRelevanceP90xZipSiteLevel",

        $"MatchedFpdRelevanceP90xOriginal_NeoScore",
        $"MatchedTpdRelevanceP90xOriginal_NeoScore"
      )
      .collect()

    input should contain theSameElementsAs (
      Seq(
        Row(
          "BidRequestId1",
          "AdvertiserId1",
          "CampaignId1",
          "AdGroupId1",
          false,
          1.0f,
          0.9f,
          "SeedId1",
          2,
          "(1000,0.1),(1001,0.2),(1002,0.3),(1003,0.4),(1004,0.5),(1005,0.6),(1006,0.7),(1007,0.8),(1008,0.9),(1009,1.0),(2000,1.1),(2001,1.2),(2002,1.3),(2003,1.4),(2004,1.5),(2005,1.6),(2006,1.7),(2007,1.8),(2008,1.9),(2009,2.0)",
          "(1000,0.1),(1001,0.2),(1002,0.3),(1003,0.4),(1004,0.5),(1005,0.6),(1006,0.7),(1007,0.8),(1008,0.9),(1009,1.0)",

          0.5f,
          0.9f,
          5.5f,
          0.30276504f,
          10,

          1.5f,
          1.9f,
          15.5f,
          0.30276504f,
          10,

          1.8f,
          3.8f,

          0.80999994f,
          1.7099999f
        ),
      )
      )
  }
}
