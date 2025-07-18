package com.thetradedesk.audience.jobs

import com.thetradedesk.audience.datasets.{CrossDeviceVendor, DataSource}
import com.thetradedesk.audience.jobs.modelinput.rsmv2.usersampling.SIBSampler.{_isDeviceIdSampledNPercent, isDeviceIdSampled1Percent}
import com.thetradedesk.audience.{date, dateFormatter, shouldTrackTDID, ttdEnv}
import com.thetradedesk.spark.TTDSparkContext.spark
import com.thetradedesk.spark.TTDSparkContext.spark.implicits._
import com.thetradedesk.spark.util.TTDConfig.config
import com.thetradedesk.spark.util.prometheus.PrometheusClient
import org.apache.spark.sql.{DataFrame, Row, SaveMode}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.{ArrayType, FloatType, IntegerType}

import scala.collection.mutable
import java.util.UUID
import java.nio.ByteBuffer
import java.security.MessageDigest

object TdidEmbeddingDotProductGeneratorOOS {
  val prometheus = new PrometheusClient("AudienceModelJob", "TdidEmbeddingDotProductGeneratorOOS")

  val salt="TRM"
  val dateStr = date.format(dateFormatter)
  val tdid_emb_path= config.getString(
  "tdid_emb_path", s"s3://thetradedesk-mlplatform-us-east-1/data/${ttdEnv}/audience/RSMV2/emb/agg/v=1/date=${dateStr}/")
  val seed_emb_path= config.getString(
  "seed_emb_path", s"s3://thetradedesk-mlplatform-us-east-1/configdata/test/audience/embedding/RSMV2/RSMv2SensitiveDensityTest/v=1/${dateStr}000000/")
  val density_feature_path= config.getString(
  "density_feature_path", s"s3://thetradedesk-mlplatform-us-east-1/features/feature_store/prod/profiles/source=bidsimpression/index=TDID/job=DailyTDIDDensityScoreSplitJob/v=1/date=${dateStr}/")
  val policy_table_path = config.getString(
  "policy_table_path", s"s3://thetradedesk-mlplatform-us-east-1/configdata/prod/audience/policyTable/RSM/v=1/${dateStr}000000/")
  val seed_id_path = config.getString(
  "seed_id_path", s"s3://thetradedesk-mlplatform-us-east-1/data/${ttdEnv}/audience/scores/seedids/v=2/date=${dateStr}/")
  val out_path = config.getString("out_path", s"s3://thetradedesk-mlplatform-us-east-1/data/${ttdEnv}/audience/scores/tdid2seedid_raw/v=1/date=${dateStr}/") // "s3://thetradedesk-mlplatform-us-east-1/users/youjun.yuan/rsmv2/emb/tdid2seedid/"
  val density_split = config.getInt("density_split", -1)
  val density_limit = config.getInt("density_limit", -1)
  val tdid_limit = config.getInt("tdid_limit", -1)
  val debug = config.getBoolean("debug", false)
  val partition = config.getInt("partition", -1)
  val EmbeddingSize = 64
  val sensitiveModel = config.getBoolean("sensitiveModel", true)
  val minMaxSeedEmb = config.getDouble("minMaxSeedEmb", 1e-6)
  val r = config.getDouble("r", 1e-8f).toFloat
  val loc_factor = config.getDouble("loc_factor", 0.8f).toFloat
  val samplingRate = config.getInt("sampling_rate", 3)

  val sigmoid = (x: Float) => (1.0f / (1.0f + math.exp(-x))).toFloat

  val embeddings2scores = (bdEmb: Seq[Float], seedEmb: Seq[Float], index: Int) => {
    var sum = 0f
    for (i <- 0 until EmbeddingSize) {
      sum = sum + bdEmb(i) * seedEmb(i + index)
    }
    sigmoid(sum)
  }

  case class SeedScore(SeedId:String, Score:Float)


  def convertUID2ToGUID(uid2: String) = {
    try {
      val md5 = MessageDigest.getInstance("MD5")
      val hashBytes = md5.digest(uid2.getBytes("ASCII"))

      val bb = ByteBuffer.wrap(hashBytes)
      val high = bb.getLong
      val low = bb.getLong

      val uuid = new UUID(high, low)
      uuid.toString
    } catch {
      case _: Exception => null
    }
  }
  val convertUID2ToGUIDUDF = udf(convertUID2ToGUID _)

  private def makeSampleUdf(n: Int) = udf((id: String) => _isDeviceIdSampledNPercent(id, n))

  /////
  def runETLPipeline(): Unit = {
    val df_seed_emb = spark.read.format("parquet").load(seed_emb_path)
      .withColumn("maxEmbedding", array_max('Embedding))
      .filter('maxEmbedding > lit(minMaxSeedEmb))
      .drop("maxEmbedding")
      .filter("PopulationRelevance is not null and MinScore is not null and MaxScore is not null and (MaxScore - MinScore) > 0.000001")
    val df_density_features = new Array[DataFrame](10)
    for (i <- 0 until 10) {
      df_density_features(i) = if (density_limit > 1) spark.read.format("parquet").load(density_feature_path + s"split=${i}/").limit(density_limit) else spark.read.format("parquet").load(density_feature_path + s"split=${i}/")
    }
    val df_tdid_embs = new Array[DataFrame](10)
    for (i <- 0 until 10) {
      df_tdid_embs(i) = if (tdid_limit > 1) spark.read.format("parquet").load(tdid_emb_path + s"split=${i}/").limit(tdid_limit) else spark.read.format("parquet").load(tdid_emb_path + s"split=${i}/").where(shouldTrackTDID('TDID) && substring('TDID, 9, 1) === lit("-"))
    }

    val df_sensitive_synthetic_ids = spark.read.parquet(policy_table_path)
      .where('Source === lit(DataSource.Seed.id) && 'CrossDeviceVendorId === lit(CrossDeviceVendor.None.id) && 'Tag === lit(4) && 'ExtendedActiveSize >= lit(2000))
      .select('SourceId.as("SeedId"), 'SyntheticId.cast(IntegerType).as("SyntheticId"), 'IsSensitive, 'ExtendedActiveSize)

    val seedEmb = df_seed_emb.drop("IsSensitive").withColumn("SyntheticId", 'SyntheticId.cast(IntegerType)).join(df_sensitive_synthetic_ids, Seq("SyntheticId"), "inner")
      .withColumn("ActiveSize", col("ExtendedActiveSize"))
      .select('SeedId, 'SyntheticId, 'IsSensitive, 'Embedding, 'ActiveSize, 'ExtendedActiveSize,'PopulationRelevance, 'MinScore, 'MaxScore,
        coalesce('LocationFactor, lit(loc_factor)).alias("LocationFactor"),
        coalesce('BaselineHitRate, lit(r)).alias("BaselineHitRate")
      ) //"SeedId",
      .as[SyntheticEmbeddingOOS]
      .collect()

    val seedIds = seedEmb.map(_.SeedId)
    val activeSizes = seedEmb.map(_.ActiveSize)
    val extendedActiveSizes = seedEmb.map(_.ExtendedActiveSize)
    val populationRelevances = seedEmb.map(_.PopulationRelevance)
    val minScores = seedEmb.map(_.MinScore)
    val maxScores = seedEmb.map(_.MaxScore)
    val locationFactors = seedEmb.map(_.LocationFactor)
    val baselineHitRates = seedEmb.map(_.BaselineHitRate)

    // Write activeSizes as both ActiveSize and ExtendedActive size to avoid breaking downstream jobs that might rely on the old
    // ActiveSize column
    val df = Seq((seedIds, activeSizes, extendedActiveSizes, populationRelevances, minScores, maxScores, locationFactors, baselineHitRates))
      .toDF("SeedId", "ActiveSize", "ExtendedActiveSize", "PopulationRelevance", "MinScore", "MaxScore", "LocationFactor", "BaselineHitRate")

    df.write.mode(SaveMode.Overwrite).parquet(seed_id_path)

    val df_seed_emb_sensitivity = spark.sparkContext.broadcast(
      seedEmb.map(e => (e.SyntheticId, e.Embedding, e.IsSensitive))
        .toSeq
    )

    val relevanceScoreUDF = udf(
      (bdSenEmb: Array[Float], bdNonSenEmb: Array[Float], SyntheticIdsLevel1: Array[Int], SyntheticIdsLevel2: Array[Int], SeedSyntheticIdsLevel1: Array[Int], SeedSyntheticIdsLevel2: Array[Int]) => {
        val syntheticIdToLevel = mutable.HashMap[Int, Int]()
        if (SyntheticIdsLevel2.nonEmpty) SyntheticIdsLevel2.foreach(e => syntheticIdToLevel.put(e, 2 * EmbeddingSize))
        if (SeedSyntheticIdsLevel2.nonEmpty) SeedSyntheticIdsLevel2.foreach(e => syntheticIdToLevel.put(e, 2 * EmbeddingSize))

        if (SyntheticIdsLevel1.nonEmpty) SyntheticIdsLevel1.foreach(e => if (!syntheticIdToLevel.contains(e)) syntheticIdToLevel.put(e, 1 * EmbeddingSize))
        if (SeedSyntheticIdsLevel1.nonEmpty) SeedSyntheticIdsLevel1.foreach(e => if (!syntheticIdToLevel.contains(e)) syntheticIdToLevel.put(e, 1 * EmbeddingSize))
        df_seed_emb_sensitivity
          .value
          .map(
            e => {
              val offset = syntheticIdToLevel.getOrElse(e._1, 0)
              if (sensitiveModel) {
                // skip embedding 0, only use embedding 1-3
                val pair = if (e._3)
                  (embeddings2scores(bdSenEmb, e._2, offset), offset / EmbeddingSize) // sensitive advertiser
                else
                  (embeddings2scores(bdNonSenEmb, e._2, offset), offset / EmbeddingSize) // pair._2 is the density level

                pair._1
              } else {
                // non sensitive model, for sensitive advertiser, always use embedding 0
                val pair = if (e._3)
                  (embeddings2scores(bdSenEmb, e._2, 0), offset / EmbeddingSize) // sensitive advertiser, always 0 embedding
                else
                  (embeddings2scores(bdNonSenEmb, e._2, offset + EmbeddingSize), offset / EmbeddingSize) // note: offset is 2 for level2, the corresponding offset should add another EmbeddingSize

                pair._1
              }
            }
          )
      }
    )




    val isIdSampled = makeSampleUdf(samplingRate)
    (0 to 9).filter(density_split < 0 || _ == density_split).foreach(i => {
      //val i = 1
      print(f"Process split ${i}")

      // without 'TDID
      val dataset_prepare_to_calculate = df_tdid_embs(i).select('TDID.as("TDID"), 'cnt, 'sen_pred_avg.cast(ArrayType(FloatType)).as("bdSenEmb"), 'non_sen_pred_avg.cast(ArrayType(FloatType)).as("bdNonSenEmb"))

      df_density_features(i) = df_density_features(i)
        .filter(isIdSampled('TDID))
        .select("TDID", "SyntheticId_Level1", "SyntheticId_Level2")

      val df_final_with_seed_density = dataset_prepare_to_calculate
        .join(
          df_density_features(i), Seq("TDID"), "left"
        )

      //:- Project [SiteZipHashed#162, TDID#529, cnt#159L, bdSenEmb#530, bdNonSenEmb#531, SyntheticId_Level1#41, SyntheticId_Level2#39, SyntheticIdLevel1#352, SyntheticIdLevel2#353]
      val df_final =
        df_final_with_seed_density
        .withColumn("SyntheticIdLevel1", lit(null)).withColumn("SyntheticIdLevel2", lit(null))
        .select('TDID, relevanceScoreUDF('bdSenEmb, 'bdNonSenEmb, coalesce('SyntheticId_Level1, typedLit(Array.empty[Int])).as("SyntheticId_Level1"), coalesce('SyntheticId_Level2, typedLit(Array.empty[Int])).as("SyntheticId_Level2")
          , coalesce('SyntheticIdLevel1, typedLit(Array.empty[Int])), coalesce('SyntheticIdLevel2, typedLit(Array.empty[Int]))).as("Score"), 'cnt)

      df_final.write.format("parquet")
        .option("codec", "org.apache.hadoop.io.compress.GzipCodec")
        .mode("overwrite")
        .save(out_path + f"split=${i}/")
    })
  }

  def main(args: Array[String]): Unit = {
    runETLPipeline()
    prometheus.pushMetrics()
  }
}

case class SyntheticEmbeddingOOS(SeedId: String, SyntheticId: Int, IsSensitive: Boolean, Embedding: Array[Float], ActiveSize: Long, ExtendedActiveSize: Long, PopulationRelevance: Float, MinScore: Float, MaxScore: Float, LocationFactor: Float, BaselineHitRate: Float)
