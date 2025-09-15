package com.thetradedesk.audience.jobs

import com.thetradedesk.audience._
import com.thetradedesk.audience.configs.AudienceModelInputGeneratorConfig
import com.thetradedesk.audience.datasets._
import com.thetradedesk.audience.jobs.PopulationInputDataGeneratorJob.prometheus
import com.thetradedesk.audience.jobs.modelinput.rsmv2.RSMV2SharedFunction.paddingColumns
import com.thetradedesk.confetti.AutoConfigResolvingETLJobBase
import com.thetradedesk.featurestore.data.cbuffer.SchemaHelper.{CBufferDataFrameReader, CBufferDataFrameWriter}
import com.thetradedesk.spark.TTDSparkContext.spark
import com.thetradedesk.spark.TTDSparkContext.spark.implicits._
import com.thetradedesk.spark.util.TTDConfig.config
import com.thetradedesk.spark.util.prometheus.PrometheusClient
import org.apache.spark.sql.SaveMode
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._

import java.time.LocalDate
import java.time.format.DateTimeFormatter
import java.util.concurrent.ThreadLocalRandom
import scala.collection.mutable

case class PopulationInputDataGeneratorJobConfig(
                                                  model: String,
                                                  inputDataS3Bucket: String,
                                                  inputDataS3Path: String,
                                                  customInputDataPath: Option[String],
                                                  subFolderKey: String,
                                                  subFolderValue: String,
                                                  syntheticIdLength: Int,
                                                  audienceResultCoalesce: Int,
                                                  populationOutputData3Path: String,
                                                  populationOutputCBData3Path: String,
                                                  runDate: LocalDate,
                                                  maxChunkRecordCount: Int,
                                                  paddingColumns: Seq[String]
                                                )

object PopulationInputDataGeneratorJob
  extends AutoConfigResolvingETLJobBase[PopulationInputDataGeneratorJobConfig](
    groupName = "audience",
    jobName = "PopulationInputDataGeneratorJob") {

  override val prometheus: Option[PrometheusClient] =
    Some(new PrometheusClient("AudiencePopulationDataJob", "PopulationInputDataGeneratorJob"))

  override def runETLPipeline(): Unit = {
    val conf = getConfig
    val date = conf.runDate

    RSMPopulationInputDataGenerator.generatePopulationData(date, conf)
  }

  /**
   * for backward compatibility, local test usage.
   * */
  override def loadLegacyConfig(): PopulationInputDataGeneratorJobConfig = ???
}


abstract class PopulationInputDataGenerator(prometheus: PrometheusClient) {

  val jobRunningTime = prometheus.createGauge(s"audience_Population_input_data_generation_job_running_time", "RSMPopulationInputDataGenerator running time", "date")
  val resultTableSize = prometheus.createGauge(s"audience_Population_input_data_generation_size", "RSMPopulationInputDataGenerator table size", "date")
  val sampleUDF = shouldConsiderTDID3(config.getInt("hitRateUserDownSampleHitPopulation", default = 1000000), config.getString("saltToSampleHitRate", default = "0BgGCE"))(_)

  def generatePopulationData(date: LocalDate, conf: PopulationInputDataGeneratorJobConfig): Unit = {

    val start = System.currentTimeMillis()

    val formatter = DateTimeFormatter.ofPattern("yyyyMMdd")
    val dateTime = date.atStartOfDay()
    val basePath = "s3://" + conf.inputDataS3Bucket + "/" + conf.inputDataS3Path

    val windowSpec = Window.partitionBy("TDID").orderBy(rand())
    //
    val impData = spark.read.cb(s"$basePath/${date.format(formatter)}000000").drop("Targets", "SyntheticIds", "ZipSiteLevel_Seed", "SampleWeights")
//      .filter(sampler.samplingFunction('TDID))
      .withColumn("rn", row_number().over(windowSpec))
      .filter(col("rn") === 1)
      .drop("rn")

    val policyTable = AudienceModelPolicyReadableDataset(AudienceModelInputGeneratorConfig.model)
      .readSinglePartition(dateTime)(spark)
      .filter((col("CrossDeviceVendorId") === 0) && (col("IsActive") === true))
      .select("SourceId", "SyntheticId", "ExtendedActiveSize")
      .withColumnRenamed("SourceId", "SeedId")

    val syntheticidsCandidates = policyTable.select('SyntheticId).as[Integer].collect()
    val bcSyntheticidsCandidates = spark.sparkContext.broadcast(syntheticidsCandidates)
    val syntheticIdToSeedId = spark.sparkContext.broadcast(policyTable.select('SyntheticId, 'SeedId).as[(Integer, String)].collect().toMap)

//    val featureStoreUser = TDIDDensityScoreReadableDataset().readPartition(date.minusDays(1), subFolderKey = Some("split"), subFolderValue = Some(RSMV2PopulationUserSampleIndex))(spark)
    val featureStoreUser = TDIDDensityScoreReadableDataset().readPartition(date.minusDays(1))(spark)
                              .select('TDID, 'SyntheticId_Level1, 'SyntheticId_Level2)

//    val featureStorePartitionCount = 20480
//
//    val featureStoreSeed = SeedDensityScoreReadableDataset()
//      .readPartition(date.minusDays(1))(spark)
//      .repartition(featureStorePartitionCount, 'FeatureKey, 'FeatureValueHashed)
//      .cache()
//
//    val topHashedValues = broadcast(impData
//      .groupBy('SiteZipHashed).agg(count("*").as("cnt")).orderBy('cnt.desc).limit(1000)
//      .select('SiteZipHashed.as("FeatureValueHashed"), lit("SiteZip").as("FeatureKey"))
//      .unionByName(
//        impData
//          .groupBy('AliasedSupplyPublisherIdCityHashed).agg(count("*").as("cnt")).orderBy('cnt.desc).limit(1000)
//          .select('AliasedSupplyPublisherIdCityHashed.as("FeatureValueHashed"), lit("AliasedSupplyPublisherIdCity").as("FeatureKey"))
//      )
//      .join(featureStoreSeed, Seq("FeatureKey", "FeatureValueHashed"), "left")
//    )
//
//    val topSiteZipHashed = broadcast(topHashedValues
//      .where('FeatureKey === lit("SiteZip"))
//      .select('FeatureValueHashed.as("SiteZipHashed"), 'SyntheticIdLevel2.as("SiteZipSyntheticIdLevel2"), 'SyntheticIdLevel1.as("SiteZipSyntheticIdLevel1")))
//      .cache()
//
//    val topAliasedSupplyPublisherIdCityHashed = broadcast(topHashedValues
//      .where('FeatureKey === lit("AliasedSupplyPublisherIdCity"))
//      .select('FeatureValueHashed.as("AliasedSupplyPublisherIdCityHashed"), 'SyntheticIdLevel2.as("AliasedSupplyPublisherIdCitySyntheticIdLevel2"), 'SyntheticIdLevel1.as("AliasedSupplyPublisherIdCitySyntheticIdLevel1")))
//      .cache()

    val seedData = AggregatedSeedReadableDataset()
      .readPartition(date)(spark)
      .repartition(AudienceModelInputGeneratorConfig.bidImpressionRepartitionNumAfterFilter, 'TDID)
      .filter(col("IsOriginal").isNotNull)
      .cache()
    
    val syntheticIdMaxLength = conf.syntheticIdLength

//    val densityFeatureAndSample = udf(
//      (SyntheticIdsLevel1: Array[Int], SyntheticIdsLevel2: Array[Int], SeedSyntheticIdsLevel1: Array[Int], SeedSyntheticIdsLevel2: Array[Int]) => {
//        val syntheticIdToLevel = mutable.HashMap[Int, Int]()
//        if (SyntheticIdsLevel2.nonEmpty) SyntheticIdsLevel2.foreach(e => syntheticIdToLevel.put(e, 3))
//        if (SeedSyntheticIdsLevel2.nonEmpty) SeedSyntheticIdsLevel2.foreach(e => syntheticIdToLevel.put(e, 3))
//
//        if (SyntheticIdsLevel1.nonEmpty) SyntheticIdsLevel1.foreach(e => if (!syntheticIdToLevel.contains(e)) syntheticIdToLevel.put(e, 2))
//        if (SeedSyntheticIdsLevel1.nonEmpty) SeedSyntheticIdsLevel1.foreach(e => if (!syntheticIdToLevel.contains(e)) syntheticIdToLevel.put(e, 2))
//
//        val allSyntheticids = bcSyntheticidsCandidates.value
//
//        val reservoir = allSyntheticids.take(syntheticIdMaxLength).toArray
//        val random = ThreadLocalRandom.current()
//
//        for (i <- syntheticIdMaxLength until allSyntheticids.length) {
//          val j = random.nextInt(i + 1)  // Thread-safe random selection
//          if (j < syntheticIdMaxLength) {
//            reservoir(j) = allSyntheticids(i)
//          }
//        }
//
//        reservoir
//          .map(
//            e => {
//              (e, syntheticIdToLevel.getOrElse(e, 1))
//            }
//          )
//      }
//    )

    val dfWithTDIDSeedDensity = impData
//        .select('BidRequestId,'TDID, 'SiteZipHashed, 'AliasedSupplyPublisherIdCityHashed)
                .select('BidRequestId,'TDID)
                .join(
                  featureStoreUser, Seq("TDID"), "left"
                )

//    val dfWithPartDensity =
//      dfWithTDIDSeedDensity
//        .join(
//          topSiteZipHashed
//            .select('SiteZipHashed), Seq("SiteZipHashed"), "left_anti")
//        .withColumn("FeatureKey", lit("SiteZip"))
//        .withColumnRenamed("SiteZipHashed", "FeatureValueHashed")
//        .repartition(featureStorePartitionCount, 'FeatureKey, 'FeatureValueHashed)
//        .join(
//          featureStoreSeed, Seq("FeatureKey", "FeatureValueHashed"), "left"
//        )
//        .withColumnRenamed("FeatureValueHashed", "SiteZipHashed")
//        .withColumnRenamed("SyntheticIdLevel1", "SiteZipSyntheticIdLevel1")
//        .withColumnRenamed("SyntheticIdLevel2", "SiteZipSyntheticIdLevel2")
//        .drop("FeatureKey")
//        .unionByName(
//          dfWithTDIDSeedDensity
//            .join(topSiteZipHashed, Seq("SiteZipHashed"), "inner")
//        )
//
//    val dfWithAllDensity =
//      dfWithPartDensity
//        .join(
//          topAliasedSupplyPublisherIdCityHashed
//            .select('AliasedSupplyPublisherIdCityHashed), Seq("AliasedSupplyPublisherIdCityHashed"), "left_anti")
//        .withColumn("FeatureKey", lit("AliasedSupplyPublisherIdCity"))
//        .withColumnRenamed("AliasedSupplyPublisherIdCityHashed", "FeatureValueHashed")
//        .repartition(featureStorePartitionCount, 'FeatureKey, 'FeatureValueHashed)
//        .join(
//          featureStoreSeed, Seq("FeatureKey", "FeatureValueHashed"), "left"
//        )
//        .withColumnRenamed("FeatureValueHashed", "AliasedSupplyPublisherIdCityHashed")
//        .withColumnRenamed("SyntheticIdLevel1", "AliasedSupplyPublisherIdCitySyntheticIdLevel1")
//        .withColumnRenamed("SyntheticIdLevel2", "AliasedSupplyPublisherIdCitySyntheticIdLevel2")
//        .drop("FeatureKey")
//        .unionByName(
//          dfWithPartDensity
//            .join(topAliasedSupplyPublisherIdCityHashed, Seq("AliasedSupplyPublisherIdCityHashed"), "inner")
//        )
//        .withColumn("SyntheticIdLevel1", concat(coalesce('SiteZipSyntheticIdLevel1, typedLit(Array.empty[Int])), coalesce('AliasedSupplyPublisherIdCitySyntheticIdLevel1, typedLit(Array.empty[Int]))))
//        .withColumn("SyntheticIdLevel2", concat(coalesce('SiteZipSyntheticIdLevel2, typedLit(Array.empty[Int])), coalesce('AliasedSupplyPublisherIdCitySyntheticIdLevel2, typedLit(Array.empty[Int]))))
//          .select('BidRequestId,
//                  'TDID,
//                  densityFeatureAndSample(coalesce('SyntheticId_Level1, typedLit(Array.empty[Int])).as("SyntheticId_Level1"),
//                                          coalesce('SyntheticId_Level2, typedLit(Array.empty[Int])).as("SyntheticId_Level2"),
//                                          coalesce('SyntheticIdLevel1, typedLit(Array.empty[Int])).as("SeedSyntheticIdsLevel1"),
//                                          coalesce('SyntheticIdLevel2, typedLit(Array.empty[Int])).as("SeedSyntheticIdsLevel2")).as("syntheticIdToDensityFeature"))
//          .select('BidRequestId, 'TDID, col("syntheticIdToDensityFeature._1").as("SyntheticIds"), col("syntheticIdToDensityFeature._2").as("ZipSiteLevel_Seed"))


    val extractTargetsUDF = udf((seedIds: Array[String], syntheticIds: Array[Int]) => {
      val syntheticIdToSeedIdMap = syntheticIdToSeedId.value
      val seedIdSet = seedIds.toSet
            syntheticIds.map(
              e => if (seedIdSet.contains(syntheticIdToSeedIdMap(e))) 1f else 0f
            )
    })

    val densityFeatureAndSample2 = udf(
      (SyntheticIdsLevel1: Array[Int], SyntheticIdsLevel2: Array[Int]) => {
        val syntheticIdToLevel = mutable.HashMap[Int, Int]()
        if (SyntheticIdsLevel2.nonEmpty) SyntheticIdsLevel2.foreach(e => syntheticIdToLevel.put(e, 3))
        if (SyntheticIdsLevel1.nonEmpty) SyntheticIdsLevel1.foreach(e => if (!syntheticIdToLevel.contains(e)) syntheticIdToLevel.put(e, 2))

        val allSyntheticids = bcSyntheticidsCandidates.value

        val reservoir = allSyntheticids.take(syntheticIdMaxLength).toArray
        val random = ThreadLocalRandom.current()

        for (i <- syntheticIdMaxLength until allSyntheticids.length) {
          val j = random.nextInt(i + 1)  // Thread-safe random selection
          if (j < syntheticIdMaxLength) {
            reservoir(j) = allSyntheticids(i)
          }
        }

        reservoir
          .map(
            e => {
              (e, syntheticIdToLevel.getOrElse(e, 1))
            }
          )
      }
    )

    val labels = dfWithTDIDSeedDensity
.select('BidRequestId,
  'TDID,
  densityFeatureAndSample2(coalesce('SyntheticId_Level1, typedLit(Array.empty[Int])).as("SyntheticId_Level1"),
    coalesce('SyntheticId_Level2, typedLit(Array.empty[Int])).as("SyntheticId_Level2")
  ).as("syntheticIdToDensityFeature"))
      .select('BidRequestId, 'TDID, col("syntheticIdToDensityFeature._1").as("SyntheticIds"), col("syntheticIdToDensityFeature._2").as("ZipSiteLevel_Seed"))

//    val labels = dfWithAllDensity
      .join(seedData.filter(size('SeedIds) > 0).select('TDID, 'SeedIds), Seq("TDID"), "left")
      .withColumn("SeedIds", coalesce('SeedIds, typedLit(Array.empty[String])))
      .withColumn("Targets", extractTargetsUDF('SeedIds, 'SyntheticIds))
      .withColumn("SampleWeights", array_repeat(lit(1.0f), syntheticIdMaxLength))
      .drop("SeedIds", "TDID")
    
    
    val result = impData.join(labels,Seq("BidrequestId"),"inner")

    val resultSet = paddingColumns(result.toDF(), conf.paddingColumns, 0).cache

    resultSet.repartition(conf.audienceResultCoalesce)
        .write.mode(SaveMode.Overwrite)
        .option("maxChunkRecordCount", conf.maxChunkRecordCount)
        .cb(conf.populationOutputCBData3Path)

    resultSet.repartition(conf.audienceResultCoalesce)
        .write.mode(SaveMode.Overwrite)
        .format("tfrecord")
        .option("recordType", "Example")
        .option("codec", "org.apache.hadoop.io.compress.GzipCodec")
        .save(conf.populationOutputData3Path)

    resultTableSize.labels(dateTime.toLocalDate.toString).set(result.count())
    jobRunningTime.labels(dateTime.toLocalDate.toString).set(System.currentTimeMillis() - start)
  }
}

object RSMPopulationInputDataGenerator extends PopulationInputDataGenerator(prometheus.get) {
}