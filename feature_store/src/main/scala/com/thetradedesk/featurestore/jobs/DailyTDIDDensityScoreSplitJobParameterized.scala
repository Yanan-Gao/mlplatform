package com.thetradedesk.featurestore.jobs

import com.thetradedesk.featurestore._
import com.thetradedesk.featurestore.rsm.CommonEnums.{CrossDeviceVendor, DataSource}
import com.thetradedesk.featurestore.transform._
import com.thetradedesk.spark.TTDSparkContext.spark
import com.thetradedesk.spark.TTDSparkContext.spark.implicits._
import com.thetradedesk.spark.util.TTDConfig.config
import com.thetradedesk.spark.util.io.FSUtils
import org.apache.spark.sql.SaveMode
import org.apache.spark.sql.functions._

import java.time.LocalDate
import java.time.format.DateTimeFormatter
import scala.util.Random

object DailyTDIDDensityScoreSplitJobParameterized extends DensityFeatureBaseJob {

  override val jobName: String = "DailyTDIDDensityScoreSplitJob"

  val repartitionNum = 32768

  def readTDIDFeaturePairMappingRDD(date: LocalDate, featurePair: String, splitIndex: Int, numPartitions: Int) = {
    val featurePairToTDIDDF = spark.read.parquet(s"$MLPlatformS3Root/$ttdEnv/profiles/source=bidsimpression/index=TDID/job=DailyTDIDFeaturePairMapping/config=${featurePair}/v=1/date=${getDateStr(date)}")
      .select(s"${featurePair}Hashed", "TDID")
      .cache()

    val saltValues = (0 until numPartitions).map(lit)
    featurePairToTDIDDF
      .filter((col("TDID").substr(9, 1) === lit("-")) && (abs(xxhash64(concat(col("TDID"), lit(salt)))) % lit(10) === lit(splitIndex)))
      .select(s"${featurePair}Hashed", "TDID")
      .withColumn("random", explode(array(saltValues: _*)))
      .repartition(repartitionNum, col(s"${featurePair}Hashed"), 'random)
  }

  def readSyntheticIdDensityScore(date: LocalDate) = {
    spark.read.parquet(s"$MLPlatformS3Root/$ttdEnv/profiles/source=bidsimpression/index=FeatureKeyValue/job=DailyDensityScoreReIndexingJob/config=SyntheticIdDensityScorePolicyTableJoined/date=${getDateStr(date)}")
      .cache()
  }

  override def runTransform(args: Array[String]) = {
    val dateStr = getDateStr(date)
    val numPartitions = 10

    val maxDensityScoreAggUDF = udaf(MaxDensityScoreAgg)
    val syntheticIdDensityScore = readSyntheticIdDensityScore(date)

    val syntheticIdToMappingId =
      spark.sparkContext.broadcast(readPolicyTable(date, DataSource.Seed.id)
        .select('SyntheticId, 'MappingId.cast("short").as("MappingId"))
        .as[(Int, Short)]
        .collect()
        .toMap)

    val syntheticIdToMappingIdUdf = udf(
      (pairs: Seq[Int], maxLength: Int) =>
        {
          val result = pairs.flatMap(e => syntheticIdToMappingId.value.get(e))
          if (pairs.length <= maxLength) result
          else Random.shuffle(result).take(maxLength)
        }
    )

    val overrideOutput = config.getBoolean("overrideOutput", default = false)
    val maxNumMappingIdsInAerospike = config.getInt("maxNumMappingIdsInAerospike", default = 1500)

    def processSplit(splitIndex: Int): Unit = {
      val tdidFeaturePairMappings = featurePairStrings.map(featurePair => (featurePair, readTDIDFeaturePairMappingRDD(date, featurePair, splitIndex, numPartitions)))

      val writePath = s"$MLPlatformS3Root/$ttdEnv/profiles/source=bidsimpression/index=TDID/job=$jobName/v=1/date=$dateStr/split=$splitIndex"
      val successFile = s"$writePath/_SUCCESS"

      // skip processing this split if data from a previous run already exists
      if (!overrideOutput && FSUtils.fileExists(successFile)(spark)) {
        println(s"split ${splitIndex} data is existing")
        return
      }

      val tdidFeaturePairDensityScore = tdidFeaturePairMappings.map { case (featurePair, tdidMapping) =>
        tdidMapping.withColumnRenamed(s"${featurePair}Hashed", "FeatureValueHashed")
          .withColumn("FeatureKey", lit(featurePair))
          .join(syntheticIdDensityScore, Seq("FeatureKey", "FeatureValueHashed", "random"))
          .select('TDID, 'SyntheticIdDensityScores)
          .groupBy('TDID)
          .agg(maxDensityScoreAggUDF(col("SyntheticIdDensityScores")).as("SyntheticIdDensityScores"))
          .withColumn("SyntheticId_Level2", DensityScoreFilterUDF.apply(0.99f, 1.01f)('SyntheticIdDensityScores))
          .withColumn("SyntheticId_Level1", DensityScoreFilterUDF.apply(0.8f, 0.99f)('SyntheticIdDensityScores))
      }

      val tdidDensityScore = tdidFeaturePairDensityScore
        .reduce(_ union _)
        .groupBy('TDID)
        .agg(
          flatten(collect_list(col("SyntheticId_Level2"))).alias("SyntheticId_Level2"),
          flatten(collect_list(col("SyntheticId_Level1"))).alias("SyntheticId_Level1")
        )
        .withColumn("MappingIdLevel2", syntheticIdToMappingIdUdf('SyntheticId_Level2, lit(maxNumMappingIdsInAerospike)))
        .withColumn("MappingIdLevel1", syntheticIdToMappingIdUdf('SyntheticId_Level1, lit(maxNumMappingIdsInAerospike) - size('MappingIdLevel2)))

      tdidDensityScore.coalesce(16384).write.mode(SaveMode.Overwrite).parquet(writePath)
    }

    splitIndex.foreach(
      processSplit
    )

    syntheticIdDensityScore.unpersist()
  }
}
