package com.thetradedesk.audience.jobs

import com.thetradedesk.audience.configs.AudienceModelInputGeneratorConfig
import com.thetradedesk.audience.datasets.CrossDeviceVendor.CrossDeviceVendor
import com.thetradedesk.audience.datasets.SeedTagOperations.dataSourceCheck
import com.thetradedesk.audience.datasets.{CrossDeviceVendor, ExtendedSeedReadableDataset, _}
import com.thetradedesk.audience.jobs.AudienceModelInputGeneratorJob.prometheus
import com.thetradedesk.audience.sample.WeightSampling.{getLabels, negativeSampleUDFGenerator, positiveSampleUDFGenerator, zipAndGroupUDFGenerator}
import com.thetradedesk.audience.transform.ContextualTransform.generateContextualFeatureTier1
import com.thetradedesk.audience.transform.ExtendArrayTransforms.seedIdToSyntheticIdMapping
import com.thetradedesk.audience.utils.S3Utils
import com.thetradedesk.audience.{dateTime, seedCoalesceAfterFilter, shouldConsiderTDID3, ttdEnv}
import com.thetradedesk.geronimo.bidsimpression.schema.{BidsImpressions, BidsImpressionsSchema}
import com.thetradedesk.geronimo.shared.{GERONIMO_DATA_SOURCE, loadParquetData}
import com.thetradedesk.spark.TTDSparkContext.spark
import com.thetradedesk.spark.TTDSparkContext.spark.implicits._
import com.thetradedesk.spark.util.TTDConfig.{config, defaultCloudProvider}
import com.thetradedesk.spark.util.io.FSUtils
import com.thetradedesk.spark.util.prometheus.PrometheusClient
import org.apache.spark.sql.{Column, DataFrame, Row, SaveMode}
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.{IntegerType, StringType, StructField, StructType}
import com.thetradedesk.audience.transform._

import java.time.LocalDate
import java.time.format.DateTimeFormatter

object AudienceModelInputGeneratorJob {
  val prometheus = new PrometheusClient("AudienceModelJob", "AudienceModelInputGeneratorJob")
  val jobRunningTime = prometheus.createGauge(s"audience_etl_job_running_time", "AudienceModelInputGeneratorJob running time", "model", "date")
  val jobProcessSize = prometheus.createGauge(s"audience_etl_job_process_size", "AudienceModelInputGeneratorJob process size", "model", "date", "data_source", "cross_device_vendor")
  val posRatioGauge = prometheus.createGauge(s"pos_ratio", "pos ratio value", "model")


  object SubFolder extends Enumeration {
    type SubFolder = Value
    val Val, Holdout, Train = Value
  }

  def main(args: Array[String]): Unit = {
    val start = System.currentTimeMillis()
    runETLPipeline()
    jobRunningTime.labels(AudienceModelInputGeneratorConfig.model.toString.toLowerCase, dateTime.toLocalDate.toString).set(System.currentTimeMillis() - start)
    prometheus.pushMetrics()
  }

  def runETLPipeline(): Unit = {
    val policyTable = clusterTargetingData(AudienceModelInputGeneratorConfig.model, AudienceModelInputGeneratorConfig.supportedDataSources,
      AudienceModelInputGeneratorConfig.seedSizeLowerScaleThreshold, AudienceModelInputGeneratorConfig.seedSizeUpperScaleThreshold)
    val date = dateTime.toLocalDate

    policyTable.foreach(typePolicyTable => {
      val dataset = {
        val result = AudienceModelInputGeneratorConfig.model match {
          case Model.AEM =>
            typePolicyTable match {
              case ((DataSource.SIB, CrossDeviceVendor.None), subPolicyTable: Array[AudienceModelPolicyRecord]) =>
                AEMSIBInputGenerator.generateDataset(date, subPolicyTable)
              case ((DataSource.Conversion, CrossDeviceVendor.None), subPolicyTable: Array[AudienceModelPolicyRecord]) =>
                AEMConversionInputGenerator.generateDataset(date, subPolicyTable)
              case _ => throw new Exception(s"unsupported policy settings: Model[${Model.AEM}], Setting[${typePolicyTable._1}]")
            }
          case Model.RSM =>
            typePolicyTable match {
              case ((DataSource.Seed, crossDeviceVendor: CrossDeviceVendor), subPolicyTable: Array[AudienceModelPolicyRecord]) =>
                RSMSeedInputGenerator(crossDeviceVendor).generateDataset(date, subPolicyTable)
              case _ => throw new Exception(s"unsupported policy settings: Model[${Model.RSM}], Setting[${typePolicyTable._1}]")
            }
          case _ => throw new Exception(s"unsupported Model[${AudienceModelInputGeneratorConfig.model}]")
        }
        jobProcessSize
          .labels(AudienceModelInputGeneratorConfig.model.toString.toLowerCase, dateTime.toLocalDate.toString, typePolicyTable._1._1.toString, typePolicyTable._1._2.toString)
          .set(typePolicyTable._2.length)

        result
      }

      val resultSet = ModelFeatureTransform.modelFeatureTransform[AudienceModelInputRecord](dataset)
        .withColumn("SplitRemainder", xxhash64(concat('GroupId, lit(AudienceModelInputGeneratorConfig.saltToSplitDataset))) % AudienceModelInputGeneratorConfig.validateDatasetSplitModule)
        .withColumn("SubFolder",
          when('SplitRemainder === lit(SubFolder.Val.id), SubFolder.Val.id)
            .when('SplitRemainder === lit(SubFolder.Holdout.id), SubFolder.Holdout.id)
            .otherwise(SubFolder.Train.id))

      resultSet.cache()

      AudienceModelInputDataset(AudienceModelInputGeneratorConfig.model.toString, s"${typePolicyTable._1._1}_${typePolicyTable._1._2}").writePartition(
        resultSet.filter('SubFolder === lit(SubFolder.Val.id)).as[AudienceModelInputRecord],
        dateTime,
        subFolderKey = Some(AudienceModelInputGeneratorConfig.subFolder),
        subFolderValue = Some(SubFolder.Val.toString),
        format = Some("tfrecord"),
        saveMode = SaveMode.Overwrite
      )

      if (AudienceModelInputGeneratorConfig.persistHoldoutSet) {
        AudienceModelInputDataset(AudienceModelInputGeneratorConfig.model.toString, s"${typePolicyTable._1._1}_${typePolicyTable._1._2}").writePartition(
          resultSet.filter('SubFolder === lit(SubFolder.Holdout.id)).as[AudienceModelInputRecord],
          dateTime,
          subFolderKey = Some(AudienceModelInputGeneratorConfig.subFolder),
          subFolderValue = Some(SubFolder.Holdout.toString),
          format = Some("tfrecord"),
          saveMode = SaveMode.Overwrite
        )
      }

      AudienceModelInputDataset(AudienceModelInputGeneratorConfig.model.toString, s"${typePolicyTable._1._1}_${typePolicyTable._1._2}").writePartition(
        resultSet.filter('SubFolder === lit(SubFolder.Train.id)).as[AudienceModelInputRecord],
        dateTime,
        subFolderKey = Some(AudienceModelInputGeneratorConfig.subFolder),
        subFolderValue = Some(SubFolder.Train.toString),
        format = Some("tfrecord"),
        saveMode = SaveMode.Overwrite
      )

      val calculateStats = udf((array: Seq[Float]) => {
        val totalElements = array.size
        val onesCount = array.sum
        val zerosCount = totalElements - onesCount

        (totalElements, onesCount, zerosCount)
      })

      val resultDF = resultSet.filter('SubFolder === lit(SubFolder.Train.id)).as[AudienceModelInputRecord]
        .withColumn("total", calculateStats(col("Targets")).getItem("_1"))
        .withColumn("pos", calculateStats(col("Targets")).getItem("_2"))
        .withColumn("neg", calculateStats(col("Targets")).getItem("_3"))
      resultDF.cache()
      val total = resultDF.select(sum("total")).cache().head().getLong(0)
      val pos_ratio = resultDF.select(sum("pos")).head().getDouble(0) / total
      MetadataDataset().writeRecord(total, dateTime,"metadata", "Count")
      MetadataDataset().writeRecord(pos_ratio, dateTime,"metadata", "PosRatio")
      posRatioGauge.labels(AudienceModelInputGeneratorConfig.model.toString.toLowerCase).set(pos_ratio)

      resultDF.unpersist()
    }
    )
  }

  def clusterTargetingData(model: Model.Value, supportedDataSources: Array[Int], seedSizeLowerScaleThreshold: Int, seedSizeUpperScaleThreshold: Int):
  Map[(DataSource.DataSource, CrossDeviceVendor.CrossDeviceVendor), Array[AudienceModelPolicyRecord]] = {
    val policyTable = AudienceModelPolicyReadableDataset(model)
      .readSinglePartition(dateTime)(spark)
      .where((length('ActiveSize)>=seedSizeLowerScaleThreshold) && (length('ActiveSize)<seedSizeUpperScaleThreshold))
      // .where('SourceId==="1ufp35u0")
      .where('IsActive)
      .where('Source.isin(supportedDataSources: _*))
      .collect()

    // todo group records by targeting data size
    policyTable.groupBy(e => (DataSource(e.Source), CrossDeviceVendor(e.CrossDeviceVendorId)))
  }
}

/**
 * This is the base class for audience model training data generation
 * including AEM(audience extension model), RSM(relevance score model), etc
 */
abstract class AudienceModelInputGenerator(name: String) {
  // set the default sampling ratio as 10%
  val samplingFunction = shouldConsiderTDID3(config.getInt(s"userDownSampleHitPopulation${name}", default = 100000), config.getString(s"saltToSampleUser${name}", default = "0BgGCE"))(_)

  val mappingFunctionGenerator =
    (dictionary: Map[Any, Int]) =>
      udf((values: Array[Any]) =>
        values.filter(dictionary.contains).map(dictionary.getOrElse(_, -1)))

  val stringEqUdf = udf((l: String, r: String) => l == r)

  /**
   * Core logic to generate model training dataset should be put here
   */
  def generateDataset(date: LocalDate, policyTable: Array[AudienceModelPolicyRecord]):
  DataFrame = {
    val (sampledBidsImpressionsKeys, bidsImpressionsLong, uniqueTDIDs) = getBidImpressions(date, AudienceModelInputGeneratorConfig.lastTouchNumberInBR,
      AudienceModelInputGeneratorConfig.tdidTouchSelection)


    val rawLabels = generateLabels(date, policyTable)

    val filteredLabels = rawLabels.join(uniqueTDIDs, Seq("TDID"), "inner")


    val refinedLabels = sampleLabels(filteredLabels, policyTable)

    val roughResult = bidsImpressionsLong
      .drop("CampaignId", "LogEntryTime")
      .join(
        refinedLabels
          .join(
            sampledBidsImpressionsKeys, Seq("TDID"), "inner"),
        Seq("TDID", "BidRequestId"), "inner")
    //      .where(stringEqUdf('BidRequestId, 'BidRequestId2))

    if (AudienceModelInputGeneratorConfig.recordIntermediateResult) {
      filteredLabels.write.mode("overwrite").parquet(s"s3a://thetradedesk-mlplatform-us-east-1/data/${ttdEnv}/audience/uniEtlTestIntermediate/${name}/filteredLabels/date=${date.format(DateTimeFormatter.BASIC_ISO_DATE)}")
      rawLabels.write.mode("overwrite").parquet(s"s3a://thetradedesk-mlplatform-us-east-1/data/${ttdEnv}/audience/uniEtlTestIntermediate/${name}/rawLabels/date=${date.format(DateTimeFormatter.BASIC_ISO_DATE)}")
      refinedLabels.write.mode("overwrite").parquet(s"s3a://thetradedesk-mlplatform-us-east-1/data/${ttdEnv}/audience/uniEtlTestIntermediate/${name}/refinedLabels/date=${date.format(DateTimeFormatter.BASIC_ISO_DATE)}")
      roughResult.write.mode("overwrite").parquet(s"s3a://thetradedesk-mlplatform-us-east-1/data/${ttdEnv}/audience/uniEtlTestIntermediate/${name}/roughResult/date=${date.format(DateTimeFormatter.BASIC_ISO_DATE)}")
    
    }

    extendFeatures(refineResult(roughResult))
  }

  def refineResult(roughResult: DataFrame): DataFrame = {
    /* TODO add weighted downSample logic to refine positive label size and negative label size
     *   https://atlassian.thetradedesk.com/confluence/display/EN/ETL+and+model+training+pipline+based+on+SIB+dataset
     */
    roughResult
  }

  def extendFeatures(refinedResult: DataFrame): DataFrame = {
    generateContextualFeatureTier1(refinedResult)
  }

  def generateLabels(date: LocalDate, policyTable: Array[AudienceModelPolicyRecord]): DataFrame

  def getBidImpressions(date: LocalDate, Ntouch: Int, tdidTouchSelection: Int = 0) = {
    val bidImpressionsS3Path = BidsImpressions.BIDSIMPRESSIONSS3 + "prod/bidsimpressions/"

    val bidsImpressionsLong = loadParquetData[BidsImpressionsSchema](bidImpressionsS3Path, date, lookBack=Some(AudienceModelInputGeneratorConfig.bidImpressionLookBack), source = Some(GERONIMO_DATA_SOURCE))
      .withColumnRenamed("UIID", "TDID")
      .filter(samplingFunction('TDID))
      .select('BidRequestId, // use to connect with bidrequest, to get more features
        'AdvertiserId,
        'AdGroupId,
        'SupplyVendor,
        'DealId,
        'SupplyVendorPublisherId,
        'AliasedSupplyPublisherId,
        'Site,
        'AdWidthInPixels,
        'AdHeightInPixels,
        'Country,
        'Region,
        'City,
        'Zip, // cast to first three digits for US is enough
        'DeviceMake,
        'DeviceModel,
        'RequestLanguages,
        'RenderingContext,
        'DeviceType,
        'OperatingSystemFamily,
        'Browser,
        'sin_hour_week, // time based features sometime are useful than expected
        'cos_hour_week,
        'sin_hour_day,
        'cos_hour_day,
        'Latitude,
        'Longitude,
        'MatchedFoldPosition,
        'InternetConnectionType,
        'OperatingSystem,
        'sin_minute_hour,
        'cos_minute_hour,
        'sin_minute_day,
        'cos_minute_day,
        'CampaignId,
        'TDID,
        'LogEntryTime,
        'ContextualCategories,
      )
      // they saved in struct type
      .withColumn("OperatingSystemFamily", 'OperatingSystemFamily("value"))
      .withColumn("Browser", 'Browser("value"))
      .withColumn("RenderingContext", 'RenderingContext("value"))
      .withColumn("InternetConnectionType", 'InternetConnectionType("value"))
      .withColumn("OperatingSystem", 'OperatingSystem("value"))
      .withColumn("DeviceType", 'DeviceType("value"))
      .withColumn("AdWidthInPixels", ('AdWidthInPixels - lit(1.0)) / lit(9999.0)) // 1 - 10000
      .withColumn("AdWidthInPixels", when('AdWidthInPixels.isNotNull, 'AdWidthInPixels).otherwise(0))
      .withColumn("AdHeightInPixels", ('AdHeightInPixels - lit(1.0)) / lit(9999.0)) // 1 - 10000
      .withColumn("AdHeightInPixels", when('AdHeightInPixels.isNotNull, 'AdHeightInPixels).otherwise(0))
      .withColumn("Latitude", ('Latitude + lit(90.0)) / lit(180.0)) // -90 - 90
      .withColumn("Latitude", when('Latitude.isNotNull, 'Latitude).otherwise(0))
      .withColumn("Longitude", ('Longitude + lit(180.0)) / lit(360.0)) //-180 - 180
      .withColumn("Longitude", when('Longitude.isNotNull, 'Longitude).otherwise(0))
      .repartition(AudienceModelInputGeneratorConfig.bidImpressionRepartitionNumAfterFilter, 'TDID)
      .cache()

    val sampledBidsImpressionsKeys = ApplyNTouchOnSameTdid(
      bidsImpressionsLong.select('BidRequestId, 'TDID, 'CampaignId, 'LogEntryTime), 
      Ntouch, tdidTouchSelection)
      .cache()

    val uniqueTDIDs = sampledBidsImpressionsKeys.select('TDID).distinct().cache()

    (sampledBidsImpressionsKeys, bidsImpressionsLong, uniqueTDIDs)
  }

  def ApplyNTouchOnSameTdid(sampledBidsImpressionsKeys: DataFrame, Ntouch: Int, tdidTouchSelection: Int = 0): DataFrame = {
    val lastWindow = Window.partitionBy('TDID).orderBy('LogEntryTime.desc)
    val stepWindow = Window.partitionBy('TDID)
    val randomWindow = Window.partitionBy('TDID).orderBy('randomRow.desc)
    var touchSample: DataFrame = null
    if (tdidTouchSelection==0) {
      touchSample = sampledBidsImpressionsKeys
      .withColumn("row", row_number().over(lastWindow))
      .filter('row <= Ntouch)
      .drop('LogEntryTime)
      .drop('row)
    } else if (tdidTouchSelection==1) {
      touchSample = sampledBidsImpressionsKeys
      .withColumn("row", row_number().over(stepWindow))
      .withColumn("TDIDCount", count("TDID").over(stepWindow))
      .filter('row % ceil('TDIDCount/Ntouch.toDouble) === lit(1.0))
      .drop('LogEntryTime)
      .drop('row)
      .drop('TDIDCount)
    } else {
      touchSample = sampledBidsImpressionsKeys
      .withColumn("randomRow", rand())
      .withColumn("row", row_number().over(randomWindow))
      .filter('row <= Ntouch)
      .drop('LogEntryTime)
      .drop('row)
      .drop('randomRow)
    }
    touchSample
  }

  /** https://atlassian.thetradedesk.com/confluence/display/EN/ETL+and+model+training+pipline+based+on+SIB+dataset
   * https://atlassian.thetradedesk.com/confluence/display/EN/RSM+-+Weight+Sampling+Labels
   * */
  private def sampleLabels(labels: DataFrame, policyTable: Array[AudienceModelPolicyRecord]): DataFrame = {
    // calculate the 95 percentile of length of the id array column
    //    val label_95_pct = labels.withColumn("size", size('))
    //      .agg(percentile_approx('size, lit(0.95), lit(10000000)))
    //      .head()
    //      .getInt(0)
    //      .toDouble

    val labelDatasetSize = labels.count()
    // the default sampling ratio is 10%
    val downSampleFactor = config.getInt(s"userDownSampleHitPopulation${name}", default = 100000)/1000000.0

    val syntheticIdToPolicy = policyTable
      .map(e => (e.SyntheticId, e))
      .toMap

    val positiveSampleUDF = positiveSampleUDFGenerator(
      syntheticIdToPolicy,
      AudienceModelInputGeneratorConfig.positiveSampleUpperThreshold,
      AudienceModelInputGeneratorConfig.positiveSampleLowerThreshold,
      AudienceModelInputGeneratorConfig.positiveSampleSmoothingFactor,
      downSampleFactor
      )

    // val labelDatasetSize = (labels.count()*(1000000.0/config.getInt(s"userDownSampleHitPopulation${name}", default = 1000000))).toLong
 
    val aboveThresholdPolicyTable = policyTable
      .filter(e => e.ActiveSize >= AudienceModelInputGeneratorConfig.positiveSampleLowerThreshold)

    val negativeSampleUDF = negativeSampleUDFGenerator(
      aboveThresholdPolicyTable,
      AudienceModelInputGeneratorConfig.positiveSampleUpperThreshold,
      labelDatasetSize,
      downSampleFactor
      )

    // downsample positive labels to keep # of positive labels among targets balanced
    val labelResult = labels
      .withColumn("PositiveSamples", positiveSampleUDF('PositiveSyntheticIds))
      .withColumn("NegativeSamples", negativeSampleUDF(lit(AudienceModelInputGeneratorConfig.negativeSampleRatio) * size(col("PositiveSamples"))))
      .withColumn("NegativeSamples", array_except(col("NegativeSamples"), 'PositiveSyntheticIds))
      .withColumn("PositiveTargets", getLabels(1f)(size($"PositiveSamples")))
      .withColumn("NegativeTargets", getLabels(0f)(size($"NegativeSamples")))
      .withColumn("SyntheticIds", concat($"PositiveSamples", $"NegativeSamples"))
      .withColumn("Targets", concat($"PositiveTargets", $"NegativeTargets"))
      .select('TDID, 'GroupId, 'SyntheticIds, 'Targets)
      // partialy explode the result to keep the target array within the max length
      .withColumn("ZippedTargets", zipAndGroupUDFGenerator(AudienceModelInputGeneratorConfig.labelMaxLength)('SyntheticIds, 'Targets))
      .select(col("TDID"), 'GroupId, explode(col("ZippedTargets")).as("ZippedTargets"))
      .select(col("TDID"), 'GroupId, col("ZippedTargets").getField("_1").as("SyntheticIds"), col("ZippedTargets").getField("_2").as("Targets"))

    labelResult
  }
}

/**
 * This class is used to generate model training samples for first party pixel model
 * using seenInBidding dataset
 
 */
object AEMSIBInputGenerator extends AudienceModelInputGenerator("AEMSIB") {

  override def generateLabels(date: LocalDate, policyTable: Array[AudienceModelPolicyRecord]): DataFrame = {
    val mappingFunction = mappingFunctionGenerator(
      policyTable.map(e => (e.TargetingDataId, e.SyntheticId)).toMap)

    // FIXME use cross device SIB dataset to replace this one
    SeenInBiddingV3DeviceDataSet().readPartition(date, lookBack = Some(AudienceModelInputGeneratorConfig.seenInBiddingLookBack))(spark)
      .withColumnRenamed("DeviceId", "TDID")
      .filter(samplingFunction('TDID))
      // only support first party targeting data ids in current solution
      .withColumn("PositiveSyntheticIds", mappingFunction('FirstPartyTargetingDataIds))
      .filter(size('PositiveSyntheticIds) > AudienceModelInputGeneratorConfig.minimalPositiveLabelSizeOfSIB)
      .select('TDID, 'TDID.alias("GroupId"), 'PositiveSyntheticIds)
      .cache()
  }
}

/**
 * This class is used to generate model training samples for audience extension model
 * using conversion tracker dataset
 */
object AEMConversionInputGenerator extends AudienceModelInputGenerator("AEMConversion") {
  private val mappingSchema = StructType(
    Array(
      StructField("TrackingTagId", StringType, nullable = false),
      StructField("SyntheticId", IntegerType, nullable = false)
    ))


  override def generateLabels(date: LocalDate, policyTable: Array[AudienceModelPolicyRecord]):
  DataFrame = {
    val mappingRows = policyTable.
      map(e => Row(e.SourceId, e.SyntheticId))
      .toSeq

    val mappingDataset = broadcast(
      spark.createDataFrame(
        spark.sparkContext.parallelize(mappingRows),
        mappingSchema))

    ConversionDataset(defaultCloudProvider)
      .readRange(date.minusDays(AudienceModelInputGeneratorConfig.conversionLookBack).atStartOfDay(), date.plusDays(1).atStartOfDay())
      .select('TDID, 'TrackingTagId)
      .filter(samplingFunction('TDID))
      .repartition(AudienceModelInputGeneratorConfig.bidImpressionRepartitionNumAfterFilter, 'TDID)
      .join(mappingDataset, "TrackingTagId")
      .groupBy('TDID)
      .agg(collect_set('SyntheticId) as "PositiveSyntheticIds")
      .withColumn("GroupId", 'TDID)
      .cache()
  }
}

/**
 * This class is used to generate model training samples for relevance score model
 * using the dataset provided by seed service
 */
case class RSMSeedInputGenerator(crossDeviceVendor: CrossDeviceVendor) extends AudienceModelInputGenerator("RSMSeed") {
  lazy val groupColumn: Column = crossDeviceVendor match {
    case CrossDeviceVendor.None => col("TDID")
    case CrossDeviceVendor.IAV2Person => col("personId")
    case CrossDeviceVendor.IAV2Household => col("householdId")
    case _ => throw new RuntimeException(s"cross device vendor ${crossDeviceVendor} is not supportted!")
  }

  lazy val seedIdsColumn: Column = crossDeviceVendor match {
    case CrossDeviceVendor.None => col("SeedIds")
    case CrossDeviceVendor.IAV2Person => array_union(col("SeedIds"), col("PersonGraphSeedIds"))
    case CrossDeviceVendor.IAV2Household => array_union(col("SeedIds"), col("HouseholdGraphSeedIds"))
    case _ => throw new RuntimeException(s"cross device vendor ${crossDeviceVendor} is not supportted!")
  }

  /**
   * read seed data from s3
   * seed data should be SeedId to TDID
   *
   * @param date
   * @param policyTable
   * @return
   */
  override def generateLabels(date: LocalDate, policyTable: Array[AudienceModelPolicyRecord]):
  DataFrame = {
    val seedData = AggregatedSeedReadableDataset()
      .readPartition(date)(spark)
      .repartition(AudienceModelInputGeneratorConfig.bidImpressionRepartitionNumAfterFilter, 'TDID)
      .cache()

    val seedIdToSyntheticId = policyTable
      .filter(e => e.CrossDeviceVendorId == crossDeviceVendor.id)
      .map(e => (e.SourceId, e.SyntheticId))
      .toMap

    val mapping = seedIdToSyntheticIdMapping(seedIdToSyntheticId)

    seedData
      .select('TDID, groupColumn.alias("GroupId"), mapping(seedIdsColumn).alias("PositiveSyntheticIds"))
      .where(size('PositiveSyntheticIds) > 0)
      .cache()
  }
}