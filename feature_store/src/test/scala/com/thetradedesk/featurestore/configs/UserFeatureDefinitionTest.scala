package com.thetradedesk.featurestore.configs

import com.thetradedesk.featurestore.testutils.TTDSparkTest
import com.thetradedesk.spark.util.io.FSUtils
import upickle.default._

import scala.io.Source


class UserFeatureDefinitionTest extends TTDSparkTest {
  test("Read user feature merge definition") {
    val json = Source.fromResource("userFeatureMergeDefinition.json").mkString
    val userFeatureMergeDefinition = read[UserFeatureMergeDefinition](json)
    assert(userFeatureMergeDefinition.validate.success, userFeatureMergeDefinition.validate.message)
  }

  test("Construct user feature merge definition and validate") {
    val featureDefinition1 = FeatureDefinition("fd1", DataType.Int)
    val featureDefinition2 = FeatureDefinition("fd2", DataType.Int, arrayLength = 3)
    val featureDefinition3 = FeatureDefinition("fd3", DataType.Double, arrayLength = 1)
    val featureDefinition4 = FeatureDefinition("fd4", DataType.String, arrayLength = 1)

    val featureSourceDefinition1 = FeatureSourceDefinition("fs1", "s1", "p1", Array(featureDefinition1, featureDefinition2))
    val featureSourceDefinition2 = FeatureSourceDefinition("fs2", "s2", "p2", Array(featureDefinition3, featureDefinition4))
    val userFeatureMergeDefinition1 = UserFeatureMergeDefinition("ufmd1", "s3", Array(featureSourceDefinition1, featureSourceDefinition2))

    val json = write(userFeatureMergeDefinition1)

    println(json)

    val userFeatureMergeDefinition2 = read[UserFeatureMergeDefinition](json)

    assert(userFeatureMergeDefinition2.validate.success, userFeatureMergeDefinition2.validate.message)
  }

  test("Construct user feature merge definition and validate for production") {
    val featureDefinition1 = FeatureDefinition("seenCount1D", DataType.Int)
    val featureDefinition2 = FeatureDefinition("avgCost1D", DataType.Float)
    val featureDefinition3 = FeatureDefinition("totalCost1D", DataType.Float)
    val featureDefinition4 = FeatureDefinition("maxFloorPrice", DataType.Float)

    val featureSourceDefinition1 = FeatureSourceDefinition("bidimp", s"features/feature_store/prodTest/user_bid_impression_feature/v=1", features = Array(featureDefinition1, featureDefinition2, featureDefinition3, featureDefinition4))
    val userFeatureMergeDefinition1 = UserFeatureMergeDefinition("userFeatureMergeDef", featureSourceDefinitions = Array(featureSourceDefinition1))

    val json = write(userFeatureMergeDefinition1)

    println(json)

    val userFeatureMergeDefinition2 = read[UserFeatureMergeDefinition](json)

    assert(userFeatureMergeDefinition2.validate.success, userFeatureMergeDefinition2.validate.message)
  }
}

case class FeatureDef(string: String, b: DataType)