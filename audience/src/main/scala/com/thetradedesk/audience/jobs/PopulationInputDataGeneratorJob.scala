package com.thetradedesk.audience.jobs

import com.thetradedesk.audience.configs.AudienceModelInputGeneratorConfig
import com.thetradedesk.audience.datasets._
import com.thetradedesk.audience.{audienceResultCoalesce, ttdReadEnv, ttdWriteEnv}
import com.thetradedesk.audience.utils.Logger.Log
import com.thetradedesk.audience.utils.S3Utils
import com.thetradedesk.audience.{shouldConsiderTDID3, _}
import com.thetradedesk.audience.jobs.PopulationInputDataGeneratorJob.prometheus
import com.thetradedesk.audience.jobs.modelinput.rsmv2.RelevanceModelInputGeneratorConfig.RSMV2PopulationUserSampleIndex
import com.thetradedesk.audience.jobs.modelinput.rsmv2.usersampling.SamplerFactory
import com.thetradedesk.geronimo.bidsimpression.schema.{BidsImpressions, BidsImpressionsSchema}
import com.thetradedesk.geronimo.shared.{GERONIMO_DATA_SOURCE, loadParquetData}
import com.thetradedesk.spark.TTDSparkContext.spark
import com.thetradedesk.spark.TTDSparkContext.spark.implicits._
import com.thetradedesk.confetti.AutoConfigResolvingETLJobBase
import com.thetradedesk.spark.util.TTDConfig.{config, defaultCloudProvider}
import com.thetradedesk.spark.util.io.FSUtils
import com.thetradedesk.spark.util.prometheus.PrometheusClient
import org.apache.commons.lang3.exception.ExceptionUtils
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.{ArrayType, StringType, StructType}
import org.apache.spark.sql.{DataFrame, Dataset, Row, SaveMode, SparkSession}

import java.util.concurrent.ThreadLocalRandom
import scala.collection.mutable
import java.time.format.DateTimeFormatter
import java.time.temporal.ChronoUnit
import java.time.{LocalDate, LocalDateTime}
import scala.util.Random
// import java.io.ObjectInputFilter.Config

case class PopulationInputDataGeneratorJobConfig(
  model: String,
  inputDataS3Bucket: String,
  inputDataS3Path: String,
  populationOutputData3Path: String,
  customInputDataPath: String,
  subFolderKey: String,
  subFolderValue: String,
  syntheticIdLength: Int,
  date_time: String
)

object PopulationInputDataGeneratorJob
  extends AutoConfigResolvingETLJobBase[PopulationInputDataGeneratorJobConfig](
    env = config.getStringRequired("env"),
    experimentName = config.getStringOption("experimentName"),
    groupName = "audience",
    jobName = "PopulationInputDataGeneratorJob") {

  override val prometheus: Option[PrometheusClient] =
    Some(new PrometheusClient("AudiencePopulationDataJob", "RSMPopulationInputDataGeneratorJob"))

  override def runETLPipeline(): Map[String, String] = {
    val conf = getConfig
    val dt = LocalDateTime.parse(conf.date_time)
    date = dt.toLocalDate
    dateTime = dt

    RSMPopulationInputDataGenerator.Config.update(conf)
    RSMPopulationInputDataGenerator.generatePopulationData(date)
    Map("status" -> "success")
  }
}


abstract class PopulationInputDataGenerator(prometheus: PrometheusClient) {

  val jobRunningTime = prometheus.createGauge(s"audience_Population_input_data_generation_job_running_time", "RSMPopulationInputDataGenerator running time", "date")
  val resultTableSize = prometheus.createGauge(s"audience_Population_input_data_generation_size", "RSMPopulationInputDataGenerator table size", "date")
  val sampleUDF = shouldConsiderTDID3(config.getInt("hitRateUserDownSampleHitPopulation", default = 1000000), config.getString("saltToSampleHitRate", default = "0BgGCE"))(_)


  object Config {
    var model: String = _
    var inputDataS3Bucket: String = _
    var inputDataS3Path: String = _
    var populationOutputData3Path: String = _
    var customInputDataPath: String = _
    var subFolderKey: String = _
    var subFolderValue: String = _
    var syntheticIdLength: Int = 500

    def update(conf: PopulationInputDataGeneratorJobConfig): Unit = {
      model = conf.model
      inputDataS3Bucket = S3Utils.refinePath(conf.inputDataS3Bucket)
      inputDataS3Path = S3Utils.refinePath(conf.inputDataS3Path)
      populationOutputData3Path = S3Utils.refinePath(conf.populationOutputData3Path)
      customInputDataPath = conf.customInputDataPath
      subFolderKey = conf.subFolderKey
      subFolderValue = conf.subFolderValue
      syntheticIdLength = conf.syntheticIdLength
    }
  }


  def generatePopulationData(date: LocalDate): Unit = {

    val start = System.currentTimeMillis()

    val formatter = DateTimeFormatter.ofPattern("yyyyMMdd")
    val dateTime = date.atStartOfDay()
    val basePath = "s3://" + Config.inputDataS3Bucket + "/" + Config.inputDataS3Path
    val outputBasePath = "s3://" + Config.inputDataS3Bucket + "/" + Config.populationOutputData3Path

    val windowSpec = Window.partitionBy("TDID").orderBy(rand())
    val impData = spark.read.format("tfrecord").load(s"$basePath/${date.format(formatter)}000000").drop("Targets", "SyntheticIds")
//      .filter(sampler.samplingFunction('TDID))
      .withColumn("rn", row_number().over(windowSpec))
      .filter(col("rn") === 1)
      .drop("rn")

    val policyTable = AudienceModelPolicyReadableDataset(AudienceModelInputGeneratorConfig.model)
      .readSinglePartition(dateTime)(spark)
      .filter((col("CrossDeviceVendorId") === 0) && (col("IsActive") === true))
      .select("SourceId", "SyntheticId", "ActiveSize")
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
      .cache()
    
    val syntheticIdMaxLength = Config.syntheticIdLength 

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
      .drop("SeedIds", "TDID")
    
    
    val result = impData.join(labels,Seq("BidrequestId"),"inner")

    
    result.repartition(audienceResultCoalesce)
        .write.mode(SaveMode.Overwrite)
        .format("tfrecord")
        .option("recordType", "Example")
        .option("codec", "org.apache.hadoop.io.compress.GzipCodec")
        .save(s"$outputBasePath/${date.format(formatter)}000000/${Config.subFolderKey}=${Config.subFolderValue}")

    resultTableSize.labels(dateTime.toLocalDate.toString).set(result.count())
    jobRunningTime.labels(dateTime.toLocalDate.toString).set(System.currentTimeMillis() - start)
  }
}

object RSMPopulationInputDataGenerator extends PopulationInputDataGenerator(prometheus: PrometheusClient) {
}