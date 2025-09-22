package com.thetradedesk.audience.jobs.modelinput.relevanceboostmodel
import com.thetradedesk.audience.jobs.modelinput.rsmv2.RSMV2SharedFunction.{SubFolder}
import com.thetradedesk.audience.configs.AudienceModelInputGeneratorConfig
import com.thetradedesk.audience.datasets.{AudienceModelPolicyReadableDataset, RelevanceBoostModelInputDataset, RelevanceBoostModelInputRecord, SegmentDataSource}
import com.thetradedesk.audience._
import org.apache.spark.sql._
import com.thetradedesk.spark.TTDSparkContext.spark
import com.thetradedesk.spark.TTDSparkContext.spark.implicits._
import com.thetradedesk.spark.sql.SQLFunctions.DataSetExtensions
import com.thetradedesk.spark.util.TTDConfig.config
import org.apache.spark.sql.catalyst.encoders.ExpressionEncoder
import org.apache.spark.sql.expressions.Aggregator
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.apache.spark.sql.{DataFrame, Column}
import java.time.LocalDate
import java.time.format.DateTimeFormatter
import scala.collection.mutable
import com.tdunning.math.stats.MergingDigest
import java.nio.ByteBuffer
import com.thetradedesk.featurestore.transform.{QuantileSummaryAgg, QuantileSummaryMergeAgg}

object RelevanceBoostModelInputGeneratorJob {
  object Config {
    val dataSources = config.getString("dataSources", "Fpd,Tpd,ExtendedFpd").split(",").map(dataSource => SegmentDataSource.withName(dataSource))
    val groupCols = config.getString("groupCols", "BidRequestId,SeedId").split(",")
    val conditionCol = config.getString("conditionCol", "DataSource")
    val prefix = config.getString("prefix", "Matched")
    val valueCol = config.getString("valueCol", "RelevanceRatio")
    val idCol = config.getString("idCol", "TargetingDataId")
    val topN = config.getInt("topN", 5)
    val percentiles = config.getString("percentiles", "50,90").split(",").map(percentile => percentile.toInt)
    val log_buffer = config.getDouble("log_buffer", 1.0).toFloat
    
  }

  def getDateStr(date: LocalDate): String = {
    val dtf = DateTimeFormatter.ofPattern("yyyyMMdd")
    date.format(dtf)
  }

  val mergeTDigest = udf { (a: Array[Byte], b: Array[Byte]) =>
      (Option(a), Option(b)) match {
        case (None, None)    => null
        case (Some(x), None) => x
        case (None, Some(y)) => y
        case (Some(x), Some(y)) =>
          val da = MergingDigest.fromBytes(ByteBuffer.wrap(x))
          val db = MergingDigest.fromBytes(ByteBuffer.wrap(y))
          da.add(db) 
          val res = ByteBuffer.allocate(da.smallByteSize())
          da.asSmallBytes(res)   
          res.array()
      }
    }
  
  val getPercentiles = udf(
      (bytes: Array[Byte], percentiles: Seq[Float]) =>{
        if (bytes == null || percentiles == null) null
        else {
          val digest = MergingDigest.fromBytes(ByteBuffer.wrap(bytes))
          percentiles.map(p => p -> digest.quantile(p.toFloat/100.0)).toMap
        }
      }
    )


  def featureTransform(df: DataFrame, 
                      dataSources: Seq[SegmentDataSource.Value], 
                      groupCols: Seq[String] = Seq("BidRequestId","SeedId"),
                      conditionCol:String = "DataSource", 
                      prefix: String = "Matched", 
                      valueCol: String = "RelevanceRatio",
                      idCol: String = "TargetingDataId",
                      topN: Int = 5,
                      percentiles: Seq[Int] = Seq(50,90),
                      log_buffer: Float = 1.0f
                      ) = {
    
    val mergeTargetingDataRelevanceScores = udaf(TargetingDataRelevanceScoreAggregator)
    val mergePercentileScore = udaf(new QuantileSummaryAgg())

    val aggCols: Seq[Column] = dataSources.flatMap { ds =>
      
      val condition = col(conditionCol) === lit(ds.id)              
      val rr = when(condition, col(valueCol))
      val td = when(condition, col(idCol))

      val source = ds.toString    

      val relevanceValues = filter(collect_list(rr), x => x.isNotNull && !isnan(x))
      val sortedRelevance = sort_array(relevanceValues, asc = false)

      val topCols: Seq[Column] =
        (1 to topN).map { k =>
          coalesce(element_at(sortedRelevance, k), lit(0.0)).cast(FloatType).as(s"${prefix}${source}RelevanceTop${k}")
        }
                             
      Seq(
        mergeTargetingDataRelevanceScores(td, rr).as(s"${source}RelevanceMap"),
        mergePercentileScore(rr).as(s"${prefix}${source}RelevanceArrayBytes"),
        coalesce(sum(rr),lit(0)).cast(FloatType)
          .as(s"${prefix}${source}RelevanceSum"),
        coalesce(stddev(rr),lit(0)).cast(FloatType)
          .as(s"${prefix}${source}RelevanceStddev"),
        coalesce(count(when(condition, col(valueCol))), lit(0)).cast(FloatType)
          .as(s"${prefix}${source}Count")
        ) ++ topCols
      }

    val agg_df = df.groupBy(groupCols.map(col): _*).agg(aggCols.head, aggCols.tail: _*)

    val extendTopNCols: Seq[Column] = (1 to topN).map { k =>
      val data_sources = Seq(SegmentDataSource.Fpd.toString, SegmentDataSource.ExtendedFpd.toString)
      val colsUpToK = data_sources.flatMap(ds => (1 to k).map(i => col(s"${prefix}${ds}RelevanceTop${i}")))
      val kthLargest = element_at(sort_array(filter(array(colsUpToK: _*), x => x.isNotNull)), -k)
      kthLargest.cast(FloatType).alias(s"${prefix}${SegmentDataSource.ExtendedFpd}TopRelevance${k}")
    }

    val pctArrayCols: Seq[Column] = dataSources.map {
      ds => getPercentiles(col(s"${prefix}${ds.toString}RelevanceArrayBytes"), typedLit(percentiles))
          .alias(s"${prefix}${ds.toString}RelevancePercentiles")
    }

    def extractPercentilesCol(mapCol: Column, dataSource: String, percentiles: Seq[Int]): Seq[Column] =
      percentiles.map(p => element_at(mapCol, lit(p)).cast(FloatType) .as(s"${prefix}${dataSource}RelevanceP${p}"))

    val percentilesCols: Seq[Column] = dataSources.flatMap(ds => extractPercentilesCol(col(s"${prefix}${ds.toString}RelevancePercentiles"), ds.toString, percentiles))
    

    def combinedStddev(n1: Column, s1: Column, sum1: Column, n2: Column, s2: Column, sum2: Column): Column = {

      val N = n1 + n2

      val mu1 = when(n1 > 0.0, sum1 / n1).otherwise(lit(null).cast(FloatType))
      val mu2 = when(n2 > 0.0, sum2 / n2).otherwise(lit(null).cast(FloatType))

      val ssWithin =
        when(n1 > 1.0, (n1 - 1.0) * pow(s1, 2)).otherwise(lit(0.0)) +
        when(n2 > 1.0, (n2 - 1.0) * pow(s2, 2)).otherwise(lit(0.0))

      val ssBetween = when((n1 > 0.0) && (n2 > 0.0),
        (n1 * n2 / N) * pow(mu1 - mu2, 2)
      ).otherwise(lit(0.0))

      val varUnion = when(N > 1.0, (ssWithin + ssBetween) / (N - 1.0))
        .otherwise(lit(null).cast(FloatType))

      sqrt(varUnion)
    }

    val res = agg_df
                .withColumn( s"${prefix}${SegmentDataSource.ExtendedFpd}RelevanceArrayBytes", mergeTDigest(col(s"${prefix}${SegmentDataSource.Fpd}RelevanceArrayBytes"),col(s"${prefix}${SegmentDataSource.ExtendedFpd}RelevanceArrayBytes")))
                .withColumn(s"${prefix}${SegmentDataSource.ExtendedFpd}RelevanceStddev", combinedStddev(col(s"${prefix}${SegmentDataSource.ExtendedFpd}Count"), 
                                                                                                        col(s"${prefix}${SegmentDataSource.ExtendedFpd}RelevanceStddev"), 
                                                                                                        col(s"${prefix}${SegmentDataSource.ExtendedFpd}RelevanceSum"),
                                                                                                        col(s"${prefix}${SegmentDataSource.Fpd}Count"), 
                                                                                                        col(s"${prefix}${SegmentDataSource.Fpd}RelevanceStddev"), 
                                                                                                        col(s"${prefix}${SegmentDataSource.Fpd}RelevanceSum")))
                .withColumn(s"${prefix}${SegmentDataSource.ExtendedFpd}Count", col(s"${prefix}${SegmentDataSource.ExtendedFpd}Count") + col(s"${prefix}${SegmentDataSource.Fpd}Count"))
                .withColumn(s"${prefix}${SegmentDataSource.ExtendedFpd}RelevanceSum", col(s"${prefix}${SegmentDataSource.ExtendedFpd}RelevanceSum") + col(s"${prefix}${SegmentDataSource.Fpd}RelevanceSum"))
                
                
                
                .select((pctArrayCols ++ agg_df.columns.map(col)):_*)
                .select((percentilesCols ++ extendTopNCols ++ agg_df.columns.map(col)):_*)

    val resCols = res.columns.map(col) ++ res.schema.collect {
            case f if f.name.startsWith(prefix) && f.dataType.isInstanceOf[NumericType] => log(col(f.name) + lit(log_buffer)).cast(FloatType).as(s"f_${f.name}")
          }
    
    res.select(resCols: _*)
  }

  def transform(
                 policyTableDf: DataFrame,
                 oosPredictionDf: DataFrame,
                 geronimoDf: DataFrame,
                 lalResultsDf: DataFrame
               ): DataFrame = {

    val policyTable = policyTableDf
      .where('CrossDeviceVendorId === 0 && 'Source === 3)

    val oosPrediction = oosPredictionDf
      .select(
        $"BidRequestId",
        $"Targets"(0).cast(FloatType).as("Target"),
        $"pred"(0).cast(FloatType).as("pred"),
        $"sen_pred"(0).cast(FloatType).as("sen_pred"),
        $"SyntheticIds"(0).as("SyntheticId"),
        $"ZipSiteLevel_Seed"(0).cast(IntegerType).as("ZipSiteLevel_Seed")
      )
      .join(policyTable, Seq("SyntheticId"), "inner")
      .withColumn("Original_NeoScore", when($"IsSensitive", $"sen_pred").otherwise($"pred"))
      .filter($"Original_NeoScore".isNotNull)
      .select($"BidRequestId", $"Target", $"IsSensitive", $"Original_NeoScore", $"SourceId".as("SeedId"), $"ZipSiteLevel_Seed")


    val geronimo = geronimoDf
      .filter($"IsImp"===1)
      .select($"BidRequestId", $"AdvertiserId", $"CampaignId", $"AdGroupId", $"UserTargetingDataIds".as("TargetingDataIds"), $"AdvertiserFirstPartyDataIds")
      .withColumn("AdvertiserFirstPartyDataIds", 
                    when(col("AdvertiserFirstPartyDataIds").isNull, col("AdvertiserFirstPartyDataIds")) 
                    .otherwise(expr("array_except(AdvertiserFirstPartyDataIds, coalesce(TargetingDataIds, array()))"))
                    )

    val oos = geronimo
      .join(oosPrediction, Seq("BidRequestId"), "inner")

    val oosTargetingDataExploded = oos
      .select($"BidRequestId", $"SeedId", explode($"TargetingDataIds").as("TargetingDataId"))

    val oosAdvertiserFirstPartyDataExploded = oos
      .select($"BidRequestId", $"SeedId", explode($"AdvertiserFirstPartyDataIds").as("TargetingDataId"))

    val lalResults = lalResultsDf
      .select($"TargetingDataId", $"RelevanceRatio", $"SeedId", $"DataSource")
      .cache() 

    val oosAdvertiserFirstPartyDataExplodedWithSource = oosAdvertiserFirstPartyDataExploded
                                      .join(lalResults, Seq("SeedId", "TargetingDataId"), "left")
                                      .withColumn("DataSource", lit(SegmentDataSource.ExtendedFpd.id))
    
    val oosTargetingDataExplodedWithSource = oosTargetingDataExploded
      .join(lalResults, Seq("SeedId", "TargetingDataId"), "left")
      .withColumn("RelevanceRatio", when($"RelevanceRatio".isNotNull, $"RelevanceRatio").otherwise(lit(0.0)))

    val oosTargetingDataExplodedFull = oosAdvertiserFirstPartyDataExplodedWithSource
                                        .unionByName(oosTargetingDataExplodedWithSource)
                                        .withColumn("RelevanceRatio", when($"RelevanceRatio".isNotNull, $"RelevanceRatio").otherwise(lit(0.0)))


    val oosWithFeatures = featureTransform(oosTargetingDataExplodedFull, 
                      Config.dataSources, 
                      Config.groupCols,
                      Config.conditionCol, 
                      Config.prefix, 
                      Config.valueCol,
                      Config.idCol,
                      Config.topN,
                      Config.percentiles,
                      Config.log_buffer
                      )





      oosWithFeatures
      .join(oos, Seq("BidRequestId", "SeedId")) // join aggregation result back with oos
       // interaction features
        .withColumn(s"f_${Config.prefix}${SegmentDataSource.Fpd}RelevanceP90xOriginal_NeoScore", col(s"f_${Config.prefix}${SegmentDataSource.Fpd}RelevanceP90")*col("Original_NeoScore"))
        .withColumn(s"f_${Config.prefix}${SegmentDataSource.ExtendedFpd}RelevanceP90xOriginal_NeoScore", col(s"f_${Config.prefix}${SegmentDataSource.ExtendedFpd}RelevanceP90")*col("Original_NeoScore"))
        .withColumn(s"f_${Config.prefix}${SegmentDataSource.Tpd}RelevanceP90xOriginal_NeoScore", col(s"f_${Config.prefix}${SegmentDataSource.Tpd}RelevanceP90")*col("Original_NeoScore"))
        .withColumn("f_Logit_NeoScore", log(col("Original_NeoScore") / (lit(1.0) - col("Original_NeoScore"))).cast(FloatType))
        .withColumn("splitReminder", rand())
        .withColumn("subfolder", when(col("splitReminder")<0.8, SubFolder.Train.id).otherwise(SubFolder.Val.id))
      
  }

  def readLalResults(date: LocalDate): DataFrame = {
    val lalReadEnv = config.getString("lalReadEnv", ttdReadEnv)

    // TODO: route it to the final segment density dataset when it's ready
    val lalDateStr = getDateStr(date.minusDays(2))

    val thirdParty = spark.read.parquet(s"s3://ttd-identity/datapipeline/$lalReadEnv/models/rsm_lal/all_model_unfiltered_results/v=1/idCap=100000/date=$lalDateStr")
      .select($"SeedId", $"TargetingDataId", $"RelevanceRatio", lit(SegmentDataSource.Tpd.id).alias("DataSource"))

    val firstParty = spark.read.parquet(s"s3://ttd-identity/datapipeline/$lalReadEnv/models/rsm_lal/1p_all_model_unfiltered_results/v=1/idCap=100000/date=$lalDateStr")
      .select($"SeedId", $"TargetingDataId", $"RelevanceRatio", lit(SegmentDataSource.Fpd.id).alias("DataSource"))

    thirdParty.union(firstParty)
  }

  def readOOSPrediction(date: LocalDate): DataFrame = {
    spark.read.parquet(s"s3://thetradedesk-mlplatform-us-east-1/data/${config.getString("oosPredictionReadEnv", ttdEnv)}/audience/RSMV2/prediction/oos_data/v=1/model_version=${getDateStr(date.minusDays(1))}000000/${getDateStr(date)}000000/")
  }

  def readGeronimo(date: LocalDate): DataFrame = {
    val dateStr = getDateStr(date)
    val yyyy = dateStr.substring(0, 4)
    val mm = dateStr.substring(4, 6)
    val dd = dateStr.substring(6, 8)

    spark.read.parquet(s"s3://thetradedesk-mlplatform-us-east-1/features/data/koav4/v=1/prod/bidsimpressions/year=$yyyy/month=$mm/day=$dd/")
  }

  def main(args: Array[String]): Unit = {
    val policyTableDf = AudienceModelPolicyReadableDataset(AudienceModelInputGeneratorConfig.model)
      .readSinglePartition(dateTime)(spark)
      .select($"CrossDeviceVendorId", $"Source", $"SourceId", $"SyntheticId", $"IsSensitive")

    val oosPredictionDf = readOOSPrediction(date)

    val geronimoDf = readGeronimo(date)

    val lalDf = readLalResults(date)

    // val audienceTargetingData = AudienceTargetingDataSet().readPartition(date)(spark)

    val modelInput = transform(policyTableDf.toDF(), oosPredictionDf, geronimoDf, lalDf)
      .cache() 

    try {
      RelevanceBoostModelInputDataset("Seed_None").writePartition(
        modelInput.filter('subfolder === lit(SubFolder.Train.id)).as[RelevanceBoostModelInputRecord],
        dateTime,
        subFolderKey = Some("split"),
        subFolderValue = Some(SubFolder.Train.toString),
        saveMode = SaveMode.Overwrite
      )

      RelevanceBoostModelInputDataset("Seed_None").writePartition(
        modelInput.filter('subfolder === lit(SubFolder.Val.id)).as[RelevanceBoostModelInputRecord],
        dateTime,
        subFolderKey = Some("split"),
        subFolderValue = Some(SubFolder.Val.toString),
        saveMode = SaveMode.Overwrite
      )
    } finally {
      modelInput.unpersist()
    }

  }
}

object TargetingDataRelevanceScoreAggregator extends Aggregator[(java.lang.Long, java.lang.Double), mutable.Map[Long, Double], Map[Long, Double]] with Serializable {
  override def zero: mutable.Map[Long, Double] = mutable.Map.empty

  override def reduce(buffer: mutable.Map[Long, Double], input: (java.lang.Long, java.lang.Double)): mutable.Map[Long, Double] = {

    if (input._1 != null && input._2 != null) {
      buffer.update(input._1, input._2)
    }
    buffer
  }

  override def merge(b1: mutable.Map[Long, Double], b2: mutable.Map[Long, Double]): mutable.Map[Long, Double] = {
    b2.foreach { case (k, v) => b1.update(k, v) }
    b1
  }

  override def finish(reduction: mutable.Map[Long, Double]): Map[Long, Double] = reduction.toMap

  override def bufferEncoder: Encoder[mutable.Map[Long, Double]] = Encoders.kryo[mutable.Map[Long, Double]]
  override def outputEncoder: Encoder[Map[Long, Double]] = ExpressionEncoder.apply[Map[Long, Double]]
}
