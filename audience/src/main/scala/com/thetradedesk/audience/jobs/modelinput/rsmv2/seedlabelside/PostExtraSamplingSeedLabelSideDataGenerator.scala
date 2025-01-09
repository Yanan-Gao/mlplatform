package com.thetradedesk.audience.jobs.modelinput.rsmv2.seedlabelside

import com.thetradedesk.audience.datasets.AggregatedSeedReadableDataset
import com.thetradedesk.audience.date
import com.thetradedesk.audience.jobs.modelinput.rsmv2.RelevanceModelInputGeneratorConfig._
import com.thetradedesk.audience.jobs.modelinput.rsmv2.RSMV2SharedFunction.{getDateStr, seedIdToSyntheticIdMapping, writeOrCache}
import com.thetradedesk.audience.jobs.modelinput.rsmv2.datainterface._
import com.thetradedesk.spark.TTDSparkContext.spark
import com.thetradedesk.spark.TTDSparkContext.spark.implicits._
import org.apache.spark.sql.Dataset
import org.apache.spark.sql.expressions.{UserDefinedFunction, Window}
import org.apache.spark.sql.functions._

import java.util.concurrent.ThreadLocalRandom
import scala.collection.immutable.TreeMap
import scala.util.Random

object PostExtraSamplingSeedLabelSideDataGenerator extends SeedLabelSideDataGenerator {

  override def prepareSeedSideFeatureAndLabel(optInSeed: Dataset[OptInSeedRecord], bidSideData: Dataset[BidSideDataRecord],
                                              userFs: Dataset[UserSiteZipLevelRecord]): Dataset[SeedLabelSideDataRecord] = {
    val aggregatedSeed = AggregatedSeedReadableDataset().readPartition(date)
      .select("TDID", "SeedIds")
      .as[RSMV2AggregatedSeedRecord]

    val sampleIndicator = generateSampleIndicator(bidSideData, aggregatedSeed, optInSeed)

    val optInSeedIds = optInSeed.select("SeedId")
    // prepare negative sampling region
    val negativeWeightMap = sampleIndicator
      .select("SyntheticId", "NegativeRightBoundIndicator").collect()

    val rightBound2SynId = TreeMap[Double, Int]()(Ordering[Double]) ++ negativeWeightMap.map { row =>
      val synId = row.getInt(0)
      val rightBound = row.getDouble(1)
      rightBound -> synId
    }
    val bcTreeMap = spark.sparkContext.broadcast(rightBound2SynId)

    val sampleNegIDsUDF: UserDefinedFunction = udf((length: Double, posIds: Seq[Int]) => {
      val tm = bcTreeMap.value
      val lengthPerRow = if (ThreadLocalRandom.current().nextDouble() < (length % 1)) {
        Math.floor(length).toInt + 1
      } else {
        Math.floor(length).toInt
      }

      (0 until lengthPerRow).map { _ =>
        var chosenId = tm.iteratorFrom(ThreadLocalRandom.current().nextDouble()).next()._2
        // loop until find a neg id don't in posIds
        while (posIds.contains(chosenId)) {
          val newRand = ThreadLocalRandom.current().nextDouble()
          chosenId = tm.iteratorFrom(newRand).next()._2
        }
        chosenId
      }
    })

    // prepare positive sampling region
    val positiveSample = sampleIndicator
      .select("SeedId", "SyntheticId", "PositiveRandIndicator").collect()

    val seedIdToSyntheticId = positiveSample
      .map(e => (e.getString(0), e.getInt(1)))
      .toMap

    val syntheticIdToRandIndicator = positiveSample
      .map(e => (e.getInt(1), e.getDouble(2)))
      .toMap

    val syntheticIdToRandBC = spark.sparkContext.broadcast(syntheticIdToRandIndicator)

    def syntheticIdPosRandFilter = udf { (origin: Seq[Int]) =>
      if (origin == null) {
        Array.empty[Int]
      } else {
        origin.flatMap { item =>
          val randIndicator = syntheticIdToRandBC.value.getOrElse(item, -1.0)
          if (ThreadLocalRandom.current().nextDouble() < randIndicator) Some(item) else None
        }.toArray
      }
    }

    val seed2SynIdMapping = seedIdToSyntheticIdMapping(seedIdToSyntheticId)

    val bidSideRowCnt = bidSideData.count()
    val totalCnt = sampleIndicator.agg(sum("NegativeCount")).first().getLong(0)
    val negSizePerRow = (totalCnt.toDouble / bidSideRowCnt)

    // generate sampling data
    val posWithNeg = bidSideData.select("TDID")
      .join(aggregatedSeed, Seq("TDID"), "left")
      .withColumn("AllPositiveSyntheticIds", seed2SynIdMapping(col("SeedIds")))
      .withColumn("NegativeSyntheticIds", sampleNegIDsUDF(lit(negSizePerRow), col("AllPositiveSyntheticIds")))
      .withColumn("PositiveSyntheticIds", syntheticIdPosRandFilter(col("AllPositiveSyntheticIds")))
      .select("TDID", "PositiveSyntheticIds", "NegativeSyntheticIds")
      .as[UserPosNegSynIds]

    joinWithDensityFeature(posWithNeg, userFs)
  }

  def generateSampleIndicator(bidSideData: Dataset[BidSideDataRecord], aggregatedSeed: Dataset[RSMV2AggregatedSeedRecord], optInSeed: Dataset[OptInSeedRecord]): Dataset[SampleIndicatorRecord] = {

    val positiveCntPerSeed = bidSideData
      .join(aggregatedSeed, Seq("TDID"))
      .select("TDID", "SeedIds")
      .withColumn("SeedId", explode(col("SeedIds")))
      .groupBy("SeedId").count()

    // prepare pos randIndicator
    val indicator = positiveCntPerSeed
      .filter('count >= lowerLimitPosCntPerSeed)
      .join(optInSeed, "SeedId")
      .withColumn("PositiveRandIndicator", least(lit(upLimitPosCntPerSeed), col("count")) / col("count"))
      .withColumn("NegativeCount", least(lit(upLimitPosCntPerSeed), col("count")) * posNegRatio)


    val totalNegCnt = indicator.agg(sum("NegativeCount")).first().getLong(0)

    val wCurr = Window.orderBy("index").rowsBetween(Window.unboundedPreceding, Window.currentRow)
    val windowSpec = Window.orderBy(lit(1))
    val dfWithIndex = indicator.withColumn("index", row_number().over(windowSpec) - 1)

    // prepare neg right bound
    val sampleIndicatorFinal = dfWithIndex
      .withColumn("currSum", sum("NegativeCount").over(wCurr))
      .withColumn("NegativeRightBoundIndicator", col("currSum") / totalNegCnt)
      .select($"SeedId", $"SyntheticId", $"PositiveRandIndicator", $"count".as("PositiveCount"), $"NegativeCount", $"NegativeRightBoundIndicator")


    val dateStr = getDateStr()
    var writePath: String = null
    if (saveIntermediateResult) {
      writePath = s"s3://${intermediateResultBasePathEndWithoutSlash}/${dateStr}/sample_indicator_final"
    }
    writeOrCache(Option(writePath),
      overrideMode, sampleIndicatorFinal).as[SampleIndicatorRecord]
  }

  def joinWithDensityFeature(posWithNeg: Dataset[UserPosNegSynIds], userFs: Dataset[UserSiteZipLevelRecord]): Dataset[SeedLabelSideDataRecord] = {
    // extra sampling
    val processPosUDF = udf((posIds: Seq[Int], bestIds: Set[Int], normalIds: Set[Int], splitReminder: Int) => {
      val results = posIds.flatMap { id =>
        if (bestIds != null && bestIds.contains(id)) {
          Some((id, 2))
        } else if (normalIds != null && normalIds.contains(id)) {
          Some((id, 1))
        } else {
          if (Random.nextFloat() < extraSamplingThreshold || splitReminder < 2) Some((id, 0)) else None
        }
      }
      results.unzip
    })

    val processNegUDF = udf((negIds: Seq[Int], bestIds: Set[Int], normalIds: Set[Int], splitReminder: Int) => {
      val results = negIds.distinct.flatMap { id =>
        if (bestIds != null && bestIds.contains(id)) {
          if (Random.nextFloat() < extraSamplingThreshold || splitReminder < 2) Some((id, 2)) else None
        } else if (normalIds != null && normalIds.contains(id)) {
          Some((id, 1))
        } else {
          Some((id, 0))
        }
      }
      results.unzip
    })

    val zipAndGroupUDFGenerator =
      (maxLength: Int) =>
        udf((ids: Seq[Int], targets: Seq[Float], features: Seq[Int]) => {
          ids.zip(targets).zip(features).map {
            case ((a, b), c) => (a, b, c)
          }
            .grouped(maxLength).toArray
        })

    val seedLabelSideData = posWithNeg
      .join(userFs, Seq("TDID"), "left")
      .withColumn("SplitRemainder", abs(xxhash64(concat('TDID, lit(splitRemainderHashSalt)))) % 10)
      .withColumn("pos_result", processPosUDF(col("PositiveSyntheticIds"), col("SyntheticId_Level2"), col("SyntheticId_Level1"), col("SplitRemainder")))
      .withColumn("PositiveSyntheticIds", col("pos_result._1"))
      .withColumn("PositiveZipSiteLevel", col("pos_result._2"))
      .withColumn("neg_result", processNegUDF(col("NegativeSyntheticIds"), col("SyntheticId_Level2"), col("SyntheticId_Level1"), col("SplitRemainder")))
      .withColumn("NegativeSyntheticIds", col("neg_result._1"))
      .withColumn("NegativeZipSiteLevel", col("neg_result._2"))
      .drop("pos_result", "neg_result")
      .withColumn("PosTarget", array_repeat(lit(1f), size(col("PositiveSyntheticIds"))))
      .withColumn("NegTarget", array_repeat(lit(0f), size(col("NegativeSyntheticIds"))))
      .withColumn("SyntheticIds", concat(col("PositiveSyntheticIds"), col("NegativeSyntheticIds")))
      .withColumn("Targets", concat(col("PosTarget"), col("NegTarget")))
      .withColumn("ZipSiteLevels", concat(col("PositiveZipSiteLevel"), col("NegativeZipSiteLevel")))
      .withColumn("ZippedTargets", zipAndGroupUDFGenerator(maxLabelLengthPerRow)('SyntheticIds, 'Targets, 'ZipSiteLevels))
      .select(col("TDID"), explode(col("ZippedTargets")).as("ZippedTargets"))
      .select(col("TDID"), col("ZippedTargets").getField("_1").as("SyntheticIds"), col("ZippedTargets").getField("_2").as("Targets"), col("ZippedTargets").getField("_3").as("ZipSiteLevel_Seed"))

    seedLabelSideData.as[SeedLabelSideDataRecord]
  }

}
