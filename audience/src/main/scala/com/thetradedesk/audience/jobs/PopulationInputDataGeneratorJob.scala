package com.thetradedesk.audience.jobs

import com.thetradedesk.audience.configs.AudienceModelInputGeneratorConfig
import com.thetradedesk.audience.datasets._
import com.thetradedesk.audience.jobs.PopulationInputDataGeneratorJob.prometheus
import com.thetradedesk.audience._
import com.thetradedesk.confetti.AutoConfigResolvingETLJobBase
import com.thetradedesk.spark.TTDSparkContext.spark
import com.thetradedesk.spark.TTDSparkContext.spark.implicits._
import com.thetradedesk.spark.util.TTDConfig.config
import com.thetradedesk.spark.util.prometheus.PrometheusClient
import org.apache.spark.sql.SaveMode
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._

import java.time.format.DateTimeFormatter
import java.time.{LocalDate, LocalDateTime}
import java.util.concurrent.ThreadLocalRandom
import scala.collection.mutable

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
    runtimeConfigBasePath = config.getStringRequired("confetti_runtime_config_base_path"),
    groupName = "audience",
    jobName = "PopulationInputDataGeneratorJob") {

  override val prometheus: Option[PrometheusClient] =
    Some(new PrometheusClient("AudiencePopulationDataJob", "PopulationInputDataGeneratorJob"))

  override def runETLPipeline(): Map[String, String] = {
    val conf = getConfig
    val dt = LocalDateTime.parse(conf.date_time)
    date = dt.toLocalDate
    dateTime = dt

    RSMPopulationInputDataGenerator.generatePopulationData(date, conf)
    Map("status" -> "success")
  }
}


abstract class PopulationInputDataGenerator(prometheus: PrometheusClient) {

  val jobRunningTime = prometheus.createGauge(s"audience_Population_input_data_generation_job_running_time", "RSMPopulationInputDataGenerator running time", "date")
  val resultTableSize = prometheus.createGauge(s"audience_Population_input_data_generation_size", "RSMPopulationInputDataGenerator table size", "date")
  val sampleUDF = shouldConsiderTDID3(config.getInt("hitRateUserDownSampleHitPopulation", default = 1000000), config.getString("saltToSampleHitRate", default = "0BgGCE"))(_)

  def generatePopulationData(date: LocalDate, config: PopulationInputDataGeneratorJobConfig): Unit = {

    val start = System.currentTimeMillis()

    val formatter = DateTimeFormatter.ofPattern("yyyyMMdd")
    val dateTime = date.atStartOfDay()
    val basePath = "s3://" + config.inputDataS3Bucket + "/" + config.inputDataS3Path
    val outputBasePath = "s3://" + config.inputDataS3Bucket + "/" + config.populationOutputData3Path

    val windowSpec = Window.partitionBy("TDID").orderBy(rand())
    val impData = spark.read.format("tfrecord").load(s"$basePath/${date.format(formatter)}000000").drop("Targets", "SyntheticIds")
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

    val featureStoreUser = TDIDDensityScoreReadableDataset().readPartition(date.minusDays(1))(spark)
      .select('TDID, 'SyntheticId_Level1, 'SyntheticId_Level2)


    val seedData = AggregatedSeedReadableDataset()
      .readPartition(date)(spark)
      .repartition(AudienceModelInputGeneratorConfig.bidImpressionRepartitionNumAfterFilter, 'TDID)
      .cache()

    val syntheticIdMaxLength = config.syntheticIdLength


    val dfWithTDIDSeedDensity = impData
      .select('BidRequestId, 'TDID)
      .join(
        featureStoreUser, Seq("TDID"), "left"
      )

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
          val j = random.nextInt(i + 1) // Thread-safe random selection
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

      .join(seedData.filter(size('SeedIds) > 0).select('TDID, 'SeedIds), Seq("TDID"), "left")
      .withColumn("SeedIds", coalesce('SeedIds, typedLit(Array.empty[String])))
      .withColumn("Targets", extractTargetsUDF('SeedIds, 'SyntheticIds))
      .drop("SeedIds", "TDID")


    val result = impData.join(labels, Seq("BidrequestId"), "inner")

    result.repartition(audienceResultCoalesce)
      .write.mode(SaveMode.Overwrite)
      .format("tfrecord")
      .option("recordType", "Example")
      .option("codec", "org.apache.hadoop.io.compress.GzipCodec")
      .save(s"$outputBasePath/${date.format(formatter)}000000/${config.subFolderKey}=${config.subFolderValue}")

    resultTableSize.labels(dateTime.toLocalDate.toString).set(result.count())
    jobRunningTime.labels(dateTime.toLocalDate.toString).set(System.currentTimeMillis() - start)
  }
}

object RSMPopulationInputDataGenerator extends PopulationInputDataGenerator(prometheus.get) {
}