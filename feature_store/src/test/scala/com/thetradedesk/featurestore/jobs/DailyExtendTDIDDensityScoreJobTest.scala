package com.thetradedesk.featurestore.jobs

import com.thetradedesk.featurestore.testutils.TTDSparkTest
import com.thetradedesk.spark.TTDSparkContext.spark.implicits._
import org.apache.spark.sql.Row
import org.scalatest.matchers.should.Matchers

class DailyExtendTDIDDensityScoreJobTest extends TTDSparkTest with Matchers {

  test("extend tdid with graph features") {

    val tdidFeatureSeq = Seq(
      ("id1", Seq(1, 2), Seq(3, 4), Seq(11, 22), Seq(666011, 666022), Seq(33, 44), Seq(666033, 666044))
    )
    val tdidDensityFeature = tdidFeatureSeq.toDF(
      "TDID",
      "SyntheticId_Level1",
      "SyntheticId_Level2",
      "MappingIdLevel1",
      "MappingIdLevel1S1",
      "MappingIdLevel2",
      "MappingIdLevel2S1"
    )

    val aggregatedGroupFeaturesByTdid = Seq(
      ("id1", Seq(1, 2, 11, 12), Seq(13, 14))
    ).toDF("TDID", "Graph_SyntheticId_Level1", "Graph_SyntheticId_Level2")

    val idMapping: Map[Int, (Int, Int)] = Map(
      11 -> (55, 0),
      12 -> (65555, 1),
      13 -> (66, 0),
      14 -> (65556, 1),
    )

    val transformed = DailyExtendTDIDDensityScoreJob
      .extendDensityFeature(tdidDensityFeature, aggregatedGroupFeaturesByTdid, idMapping)
      .collect()
      .toList

    val exp = tdidFeatureSeq ++ Seq(Seq(11, 12), Seq(13, 14), Seq(55), Seq(19), Seq(66), Seq(20))
    transformed should contain theSameElementsAs Seq(
      Row("id1", Seq(1, 2), Seq(3, 4), Seq(11, 22), Seq(666011, 666022), Seq(33, 44), Seq(666033, 666044), Seq(11, 12), Seq(13, 14), Seq(55), Seq(19), Seq(66), Seq(20))
    )
  }
}
