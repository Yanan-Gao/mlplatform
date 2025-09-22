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
        1000L, 1001L, 1002L, 1003L, 1004L,
        2000L, 2001L, 2002L, 2003L, 2004L, 2005L, 2006L, 2007L, 2008L, 2009L,
      ), Seq(
        1000L, 1001L, 1002L, 1003L, 1004L, 1005L, 1006L, 1007L, 1008L, 1009L,
      ), "AdvertiserId1", "CampaignId1", "AdGroupId1")
    ).toDF("BidRequestId", "IsImp", "UserTargetingDataIds", "AdvertiserFirstPartyDataIds", "AdvertiserId", "CampaignId", "AdGroupId")

    val lalResultsDf = Seq(
      ("SeedId1", 1000L, 0.1, 2),
      ("SeedId1", 1001L, 0.2, 2),
      ("SeedId1", 1002L, 0.3, 2),
      ("SeedId1", 1003L, 0.4, 2),
      ("SeedId1", 1004L, 0.5, 2),
      ("SeedId1", 1005L, 0.6, 2),
      ("SeedId1", 1006L, 0.7, 2),
      ("SeedId1", 1007L, 0.8, 2),
      ("SeedId1", 1008L, 0.9, 2),
      ("SeedId1", 1009L, 1.0, 2),

      ("SeedId1", 2000L, 1.1, 1),
      ("SeedId1", 2001L, 1.2, 1),
      ("SeedId1", 2002L, 1.3, 1),
      ("SeedId1", 2003L, 1.4, 1),
      ("SeedId1", 2004L, 1.5, 1),
      ("SeedId1", 2005L, 1.6, 1),
      ("SeedId1", 2006L, 1.7, 1),
      ("SeedId1", 2007L, 1.8, 1),
      ("SeedId1", 2008L, 1.9, 1),
      ("SeedId1", 2009L, 2.0, 1),
    ).toDF("SeedId", "TargetingDataId", "RelevanceRatio", "DataSource")

    val stringifyMap = udf((m: Map[Long, Double]) => {
      m.toList.sortBy(_._1).mkString(",")
    })

    val input = RelevanceBoostModelInputGeneratorJob.transform(
        policyTableDf,
        oosPredictionDf,
        geronimoDf,
        lalResultsDf
      )
      .withColumn("TpdRelevanceMapStr", stringifyMap($"TpdRelevanceMap"))
      .withColumn("FpdRelevanceMapStr", stringifyMap($"FpdRelevanceMap"))
      .withColumn("ExtendedFpdRelevanceMapStr", stringifyMap($"ExtendedFpdRelevanceMap"))
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
        $"TpdRelevanceMapStr",
        $"FpdRelevanceMapStr",
        $"ExtendedFpdRelevanceMapStr",
        
        $"f_MatchedFpdRelevanceTop1",
        $"f_MatchedFpdRelevanceTop2",
        $"f_MatchedFpdRelevanceTop3",
        $"f_MatchedFpdRelevanceTop4",
        $"f_MatchedFpdRelevanceTop5",
        $"f_MatchedFpdRelevanceP50",
        $"f_MatchedFpdRelevanceP90",
        $"f_MatchedFpdRelevanceSum",
        $"f_MatchedFpdRelevanceStddev",
        $"f_MatchedFpdCount",

        $"f_MatchedTpdRelevanceTop1",
        $"f_MatchedTpdRelevanceTop2",
        $"f_MatchedTpdRelevanceTop3",
        $"f_MatchedTpdRelevanceTop4",
        $"f_MatchedTpdRelevanceTop5",

        $"f_MatchedTpdRelevanceP50",
        $"f_MatchedTpdRelevanceP90",
        $"f_MatchedTpdRelevanceSum",
        $"f_MatchedTpdRelevanceStddev",
        $"f_MatchedTpdCount",

        $"f_MatchedExtendedFpdRelevanceTop1",
        $"f_MatchedExtendedFpdRelevanceTop2",
        $"f_MatchedExtendedFpdRelevanceTop3",
        $"f_MatchedExtendedFpdRelevanceTop4",
        $"f_MatchedExtendedFpdRelevanceTop5",

        $"f_MatchedExtendedFpdRelevanceP50",
        $"f_MatchedExtendedFpdRelevanceP90",
        $"f_MatchedExtendedFpdRelevanceSum",
        $"f_MatchedExtendedFpdRelevanceStddev",
        $"f_MatchedExtendedFpdCount",

        // $"MatchedFpdRelevanceP90xZipSiteLevel",
        // $"MatchedTpdRelevanceP90xZipSiteLevel",

        $"f_MatchedTpdRelevanceP90xOriginal_NeoScore",
        $"f_MatchedFpdRelevanceP90xOriginal_NeoScore",
        $"f_MatchedExtendedFpdRelevanceP90xOriginal_NeoScore",

        $"f_Logit_NeoScore"
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
          "(2000,1.1),(2001,1.2),(2002,1.3),(2003,1.4),(2004,1.5),(2005,1.6),(2006,1.7),(2007,1.8),(2008,1.9),(2009,2.0)",
          "(1000,0.1),(1001,0.2),(1002,0.3),(1003,0.4),(1004,0.5)",
          "(1005,0.6),(1006,0.7),(1007,0.8),(1008,0.9),(1009,1.0)",


          
          0.40546509623527527f,	
          0.33647221326828003f,	
          0.2623642385005951f,	
          0.18232159316539764f,	
          0.09531020373106003f,	
          0.0f,	
          0.33647221326828003f,	
          0.9162907600402832f,	
          0.1467926800251007f,	
          1.7917594909667969f,


          1.0986123085021973f,	
          1.0647107362747192f,	
          1.0296194553375244f,	
          0.9932518005371094f,	
          0.9555113911628723,

          0.7419372797012329f,	
          1.0647107362747192f,	
          2.8033604621887207f,	
          0.26448893547058105f,	
          2.397895336151123,

          0.6931471824645996f,
          0.6418538689613342f,	
          0.5877866148948669f,	
          0.5306282639503479f,	
          0.4700036346912384f,	
          0.0f,	
          0.5306282639503479f,	
          1.8718022108078003f,	
          0.26448896527290344f,	
          2.397895336151123f,

          0.9582396149635315f,
          0.3028249740600586f,	
          0.4775654375553131f,	
          2.1972243785858154f
     
        ),
      )
      )
  }
}
