package com.thetradedesk.audience.jobs.modelinput.rsmv2.seedlabelside

import com.thetradedesk.audience.doNotTrackTDID
import com.thetradedesk.audience.jobs.modelinput.rsmv2.{BidImpSideDataGenerator, RelevanceModelInputGeneratorJobConfig}
import com.thetradedesk.audience.jobs.modelinput.rsmv2.datainterface.{BidSideDataRecord, RSMV2AggregatedSeedRecord, UserPosNegSynIds, UserSiteZipLevelRecord}
import com.thetradedesk.audience.jobs.modelinput.rsmv2.seedlabelside.PostExtraSamplingSeedLabelSideDataGenerator.BidSeedDataRecord
import com.thetradedesk.audience.transform.IDTransform.IDType
import com.thetradedesk.audience.utils.TTDSparkTest
import org.apache.spark.sql.Dataset
import org.apache.spark.sql.functions._

import java.time.LocalDate

class PostExtraSamplingSeedLabelSideDataGeneratorTest extends TTDSparkTest {
  test("test getBidImpSideData") {
    import this.spark.implicits._

    val bidRequests = Seq(
      BidSideDataRecord("1", TDID = "device1", DeviceAdvertisingId = Some("device1")),
      BidSideDataRecord("2", TDID = "uid2a", UnifiedId2 = Some("uid2a"), CookieTDID = Some("cookie1")),
      BidSideDataRecord("3", TDID = "device3", DeviceAdvertisingId = Some("device3"), CookieTDID = Some("cookie1")),
      BidSideDataRecord("4", TDID = "device1", DeviceAdvertisingId = Some("device1"))
    ).toDS()

    val config = RelevanceModelInputGeneratorJobConfig(
      modelName = "RSMV2",
      useTmpFeatureGenerator = false,
      extraSamplingThreshold = 0.05,
      rsmV2FeatureSourcePath = "/featuresV2.json",
      rsmV2FeatureDestPath = "",
      subFolder = "Full",
      optInSeedEmptyTagPath = "",
      densityFeatureReadPathWithoutSlash = "profiles/source=bidsimpression/index=TDID/job=DailyTDIDDensityScoreSplitJob/v=1",
      sensitiveFeatureColumns = Seq("Site", "Zip"),
      persistHoldoutSet = true,
      optInSeedType = "Active",
      optInSeedFilterExpr = "true",
      posNegRatio = 50,
      lowerLimitPosCntPerSeed = 200,
      RSMV2UserSampleSalt = "TRM",
      RSMV2PopulationUserSampleIndex = Seq(3, 5, 7),
      RSMV2UserSampleRatio = 1,
      samplerName = "RSMV2",
      overrideMode = false,
      splitRemainderHashSalt = "split_RSMV2",
      upLimitPosCntPerSeed = 40000,
      saveIntermediateResult = false,
      intermediateResultBasePathEndWithoutSlash = "thetradedesk-mlplatform-us-east-1/users/yixuan.zheng/allinone/dataset/Full",
      maxLabelLengthPerRow = 50,
      minRowNumsPerPartition = 100000,
      trainValHoldoutTotalSplits = 10,
      activeSeedIdWhiteList = "",
      runDate = LocalDate.parse("2025-07-25")
    )

    val bidResult = BidImpSideDataGenerator.prepareBidImpSideFeatureDataset(bidRequests, config)
    val bidSideTrainingData = bidResult.bidSideTrainingData

    val expectedBidSideTrainingData = Set(
      "device1", "uid2a", "cookie1", "device3"
    )

    bidSideTrainingData.show()
    assert(bidSideTrainingData.select("TDID").as[String].collect().toSet == expectedBidSideTrainingData)
  }

  test("test getBidSeedData") {
    import this.spark.implicits._

    val bidRequests = Seq(
      BidSideDataRecord("1", TDID = "device1", DeviceAdvertisingId = Some("device1"), CookieTDID = Some("cookie1")),
      BidSideDataRecord("2", TDID = "uid2a", DeviceAdvertisingId = Some("device2"), CookieTDID = Some(doNotTrackTDID), UnifiedId2 = Some("uid2a")),
      BidSideDataRecord("3", TDID = "device3", DeviceAdvertisingId = Some("device3")),
      BidSideDataRecord("4", TDID = "device4", DeviceAdvertisingId = Some("device4"))
    ).toDS()

    val seeds: Dataset[RSMV2AggregatedSeedRecord] = Seq(
      RSMV2AggregatedSeedRecord("device1", IDType.DeviceAdvertisingId.id, Array("seed1", "seed2")),
      RSMV2AggregatedSeedRecord("cookie1", IDType.CookieTDID.id, Array("seed2", "seed3"))
    ).toDS()

    val bidSeedData = PostExtraSamplingSeedLabelSideDataGenerator.getBidSeedData(bidRequests, seeds, seeds.select(explode('SeedIds)).distinct().as[String].collect())

    val expectedBidSeedData = Seq(
      BidSeedDataRecord("1", 0, Seq("seed1", "seed2", "seed3"), Seq("cookie1", "device1")),
      BidSeedDataRecord("2", 0, Seq(), Seq("device2", "uid2a")),
      BidSeedDataRecord("3", 0, Seq(), Seq("device3")),
      BidSeedDataRecord("4", 0, Seq(), Seq("device4")),
    )

    assert(bidSeedData.map(record => record.copy(SeedIds = record.SeedIds.sorted)).collect().toSet == expectedBidSeedData.toSet)
  }

  test("test getPositiveCntPerSeed") {
    import this.spark.implicits._

    val bidSeedData = Seq(
      BidSeedDataRecord("1", 0, Seq("seed1", "seed2"), Seq()),
      BidSeedDataRecord("2", 0, Seq("seed1"), Seq()),
      BidSeedDataRecord("3", 0, Seq("seed3"), Seq())
    ).toDS()

    val positiveCntPerSeed = PostExtraSamplingSeedLabelSideDataGenerator.getPositiveCntPerSeed(bidSeedData)
    positiveCntPerSeed.show()

    val expectedPositiveCntPerSeed = Seq(
      ("seed1", 2),
      ("seed2", 1),
      ("seed3", 1)
    ).toDF("SeedId", "count")

    assert(positiveCntPerSeed.collect().toSet == expectedPositiveCntPerSeed.collect().toSet)
  }

  test("test getBidRequestDensityFeature") {
    import this.spark.implicits._

    val userPosNegSynIds = Seq(
      UserPosNegSynIds("1", Seq("cookie1", "device1"), 0, Seq(0), Seq(0)),
      UserPosNegSynIds("2", Seq("cookie2"), 0, Seq(0), Seq(0)),
    ).toDS()

    val userFs = Seq(
      UserSiteZipLevelRecord("cookie1", Seq(1), Seq(3)),
      UserSiteZipLevelRecord("device1", Seq(3), Seq(2, 1))
    ).toDS()

    val bidRequestDensityFeature = PostExtraSamplingSeedLabelSideDataGenerator.getBidRequestDensityFeature(userPosNegSynIds, userFs)

    val expectedBidRequestDensityFeature = Seq(
      // Keep Seeds 1 and 3 as Level 2, leave Seed 1 as Level 1
      ("1", 0, Seq(0), Seq(0), Seq(1, 3), Seq(2)),
      ("2", 0, Seq(0), Seq(0), null, null)
    ).toDF("BidRequestId", "SplitRemainder", "PositiveSyntheticIds", "NegativeSyntheticIds", "SyntheticId_Level2", "SyntheticId_Level1")

    assert(bidRequestDensityFeature.collect().toSet == expectedBidRequestDensityFeature.collect().toSet)
  }
}
