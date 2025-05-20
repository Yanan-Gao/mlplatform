package com.thetradedesk.audience.jobs


import com.thetradedesk.audience.datasets.{CrossDeviceVendor, DataSource}
import com.thetradedesk.spark.TTDSparkContext.spark
import com.thetradedesk.spark.TTDSparkContext.spark.implicits._
import com.thetradedesk.spark.util.TTDConfig.config
import org.apache.spark.SparkContext
import org.apache.spark.sql.expressions.UserDefinedFunction
import org.apache.spark.sql.{DataFrame, Row, SaveMode}
import org.apache.spark.sql.functions.{e, _}
import org.apache.spark.sql.types.{ArrayType, FloatType, IntegerType}

import scala.collection.mutable
import java.util.UUID

object TdidEmbeddingDotProductGenerator {
  val salt="TRM"
  //val tdid_emb_path="s3://thetradedesk-mlplatform-us-east-1/users/youjun.yuan/rsmv2/emb/agg_tdid/"
  val tdid_emb_path="s3://thetradedesk-mlplatform-us-east-1/users/youjun.yuan/rsmv2/emb/agg_tdid/date=20250216/"
  val seed_emb_path="s3://thetradedesk-mlplatform-us-east-1/configdata/test/audience/embedding/RSMV2/RSMv2TestTreatment/v=1/20250216000000/embedding.parquet.gzip"
  val density_feature_path="s3://thetradedesk-mlplatform-us-east-1/features/feature_store/prod/profiles/source=bidsimpression/index=TDID/job=DailyTDIDDensityScoreSplitJob/v=1/date=20250216/"
  //val density_feature_path = "s3://thetradedesk-mlplatform-us-east-1/users/youjun.yuan/rsmv2/emb/density_split_1/"
//  val seed_list_path="s3://thetradedesk-mlplatform-us-east-1/data/prod/audience/relevanceseedsdataset/v=1/date=20250216/"
  val policy_table_path ="s3://thetradedesk-mlplatform-us-east-1/configdata/prod/audience/policyTable/RSM/v=1/20250216000000/"
  val seed_id_path ="s3://thetradedesk-mlplatform-us-east-1/data/dev/audience/scores/seedids/v=2/date=20250216/"
  val out_path = config.getString("TdidEmbeddingDotProductGenerator.out_path", "s3://thetradedesk-mlplatform-us-east-1/data/dev/audience/scores/tdid2seedidV2/v=1/date=20250216/") // "s3://thetradedesk-mlplatform-us-east-1/users/youjun.yuan/rsmv2/emb/tdid2seedid/"
  val avail_path = config.getString("TdidEmbeddingDotProductGenerator.avail_path", "s3://thetradedesk-mlplatform-us-east-1/data/prodTest/audience/RSMV2/Availycp-dup_plus/v=2/20250216000000/")
  val seed_density_path = config.getString("TdidEmbeddingDotProductGenerator.seed_density_path", "s3://thetradedesk-mlplatform-us-east-1/features/feature_store/prodTest/profiles/source=bidsimpression/index=FeatureKeyValue/job=DailyDensityScoreReIndexingJob/config=SyntheticIdDensityScoreCategorized/date=20250216/")
  val density_split = config.getInt("TdidEmbeddingDotProductGenerator.density_split", -1)
  val density_limit = config.getInt("TdidEmbeddingDotProductGenerator.density_limit", -1)
  val tdid_limit = config.getInt("TdidEmbeddingDotProductGenerator.tdid_limit", -1)
  val debug = config.getBoolean("TdidEmbeddingDotProductGenerator.debug", false)
  val partition = config.getInt("TdidEmbeddingDotProductGenerator.partition", -1)
  val EmbeddingSize = 64

//  val extractSubarray = udf { (group: Int, arr: Seq[Double]) =>
//    val start = group * 64
//    arr.slice(start, start + 64)
//  }

  val sigmoid = (x: Float) => (1.0f / (1.0f + math.exp(-x))).toFloat
//  val sumArray = udf((arr: Seq[Double]) => arr.sum)
//  val sumArray2 = udf((arr1: Seq[Double], arr2: Seq[Double]) => {
//    var sum:Double = 0
//    for (i <- 0 until arr1.length) {
//      sum = sum + arr1(i)*arr2(i)
//    }
//    sum
//  })
  val embeddings2scores = (bdEmb: Seq[Float], seedEmb: Seq[Float], index: Int) => {
    var sum = 0f
    for (i <- 0 until EmbeddingSize) {
      sum = sum + bdEmb(i) * seedEmb(i + index)
    }
    sigmoid(sum)
  }
  //val sigmoidUDF = udf(sigmoid)

  case class SeedScore(SeedId:String, Score:Float)
  val props2scores = udf((props: Seq[Row], syntheticIdsL1: Seq[Long], syntheticIdsL2: Seq[Long]) => {

    val l1set: Set[Long] = if (syntheticIdsL1 != null) syntheticIdsL1.toSet else Set[Long]()
    val l2set = if (syntheticIdsL2 != null) syntheticIdsL2.toSet else Set[Long]()

    val scores = props.map(row => {
      //0.0f
      val scoreArr = row.getSeq(0).toArray[Float]
      val syntheticId = row.getLong(1)
      val seedId = row.getString(2)
      if (row.get(3) != null) { // sensitive advertizer
        SeedScore(seedId, sigmoid(scoreArr(0)))
      } else {
        if (l2set.contains(syntheticId)) {
          SeedScore(seedId, sigmoid(scoreArr(3)))
        } else if (l1set.contains(syntheticId)) {
          SeedScore(seedId, sigmoid(scoreArr(2)))
        } else {
          SeedScore(seedId, sigmoid(scoreArr(1)))
        }
      }
    })

    scores

  })

  val props2scoresv2 = udf((props: Seq[Row], syntheticIdsL1: Seq[Int], syntheticIdsL2: Seq[Int], totalSynIdCnt:Int, syntheticIds2pos:Array[Int]) => {

    val l1set: Set[Int] = if (syntheticIdsL1 != null) syntheticIdsL1.toSet else Set[Int]()
    val l2set = if (syntheticIdsL2 != null) syntheticIdsL2.toSet else Set[Int]()
    var ret = new Array[Float](totalSynIdCnt)
//    val mapSynid2Scores = props.foldLeft(Map.empty[Long, Float]) { (acc, row) => {
//      val scoreArr = row.getSeq(0).toArray[Float]
//      val syntheticId = row.getLong(1)
//      var score = 0.0f
//      if (row.get(3) != null) { // sensitive advertizer
//        score = sigmoid(scoreArr(0))
//      } else {
//        if (l2set.contains(syntheticId)) {
//          score = sigmoid(scoreArr(3))
//        } else if (l1set.contains(syntheticId)) {
//          score = sigmoid(scoreArr(2))
//        } else {
//          score = sigmoid(scoreArr(1))
//        }
//      }
//      acc + (syntheticId -> score)
//    }
//    }
    props.map(row => {
      val scoreArr = row.getSeq(0).toArray[Float]
      val syntheticId = row.getInt(1)
      val pos = syntheticIds2pos(syntheticId)
      var score = 0.0f
      if (row.get(3) != null) { // sensitive advertizer
        score = sigmoid(scoreArr(0))
      } else {
        if (l2set.contains(syntheticId)) {
          score = sigmoid(scoreArr(3))
        } else if (l1set.contains(syntheticId)) {
          score = sigmoid(scoreArr(2))
        } else {
          score = sigmoid(scoreArr(1))
        }
      }

      ret(pos) = score
    })
    ret
  })


  ///// UDF sample
  case class Uuid(MostSigBits: Long, LeastSigBits: Long)

  def charToLong(char: Char): Long = {
    if (char >= '0' && char <= '9')
      char - '0'
    else if (char >= 'A' && char <= 'F')
      char - ('A' - 0xaL)
    else if (char >= 'a' && char <= 'f')
      char - ('a' - 0xaL)
    else
      0L
  }

  def guidToLongs(tdid: String): Uuid = {
    if (tdid == null) {
      Uuid(0,0)
    } else {
      val hexString = tdid.replace("-", "")

      if (!hexString.matches("[0-9A-Fa-f]{32}")) {
        Uuid(0, 0)
      } else {
        val highBits = hexString.slice(0, 16).map(charToLong).zipWithIndex.map { case (value, i) => value << (60 - (4 * i)) }.reduce(_ | _)
        val lowBits = hexString.slice(16, 32).map(charToLong).zipWithIndex.map { case (value, i) => value << (60 - (4 * i)) }.reduce(_ | _)

        Uuid(highBits, lowBits)
      }
    }
  }

  def udfGuidToLongs: UserDefinedFunction = udf((id: String) => guidToLongs(id))

  def longsToGuid(tdid1: Long, tdid2: Long): String = {
    new UUID(tdid1, tdid2).toString
  }

  def toInt32(bytes: Array[Byte], index: Int): Int = {
    val b1 = (0xff & (bytes(index) & 0xFF )) << 0
    val b2 = (0xff & (bytes(index + 1) & 0xFF )) << 8
    val b3 = (0xff & (bytes(index + 2) & 0xFF )) << 16
    val b4 = (0xff & (bytes(index + 3) & 0xFF )) << 24

    b1 | b2 | b3 | b4
  }

  private val tduBasePopulation = 1000000
  private val tduSamplePopulation = 100000
  private val tdidBasedAvailsSamplePopulation = tduBasePopulation / 30

  def udfTdidInTduSample: UserDefinedFunction = udf((mostSigBits: Long, leastSigBits: Long) => tdidInTduSample(mostSigBits, leastSigBits))


  def tdidInTduSample(mostSigBits: Long, leastSigBits: Long): Boolean = userIsSampled(mostSigBits, leastSigBits, tduBasePopulation, tduSamplePopulation)

  def tdidInAvailsSample(mostSigBits: Long, leastSigBits: Long): Boolean = userIsSampled(mostSigBits, leastSigBits, tduBasePopulation, tdidBasedAvailsSamplePopulation)

  /**
   * Calculate whether a given TDID is included in the 10% sample for TargetingDataUser
   * This is a replication of the logic that DataServer uses
   *
   * @return Boolean
   */
  def userIsSampled(mostSigBits: Long, leastSigBits: Long, basePopulation: Int, samplePopulation: Int): Boolean = {
    // the logic here is replicated from Constants.UserIsSampled from AdPlatform
    val tdidBytes = Array.fill[Byte](16)(0)

    // most sig bits first, then least sig bits
    // so the reason for this super wacky ordering is to account for the differences in how guids are represented
    // in binary in c# vs java. the 2 longs that make up TDIDs come from the java representation but the 10% sample
    // is done in DataServer from the c#  representation
    tdidBytes(3)  = ((mostSigBits >> 56) & 0xFF).asInstanceOf[Byte]
    tdidBytes(2)  = ((mostSigBits >> 48) & 0xFF).asInstanceOf[Byte]
    tdidBytes(1)  = ((mostSigBits >> 40) & 0xFF).asInstanceOf[Byte]
    tdidBytes(0)  = ((mostSigBits >> 32) & 0xFF).asInstanceOf[Byte]
    tdidBytes(5)  = ((mostSigBits >> 24) & 0xFF).asInstanceOf[Byte]
    tdidBytes(4)  = ((mostSigBits >> 16) & 0xFF).asInstanceOf[Byte]
    tdidBytes(7)  = ((mostSigBits >> 8 ) & 0xFF).asInstanceOf[Byte]
    tdidBytes(6)  = ((mostSigBits >> 0 ) & 0xFF).asInstanceOf[Byte]

    tdidBytes(8)  = ((leastSigBits >> 56) & 0xFF).asInstanceOf[Byte]
    tdidBytes(9)  = ((leastSigBits >> 48) & 0xFF).asInstanceOf[Byte]
    tdidBytes(10) = ((leastSigBits >> 40) & 0xFF).asInstanceOf[Byte]
    tdidBytes(11) = ((leastSigBits >> 32) & 0xFF).asInstanceOf[Byte]
    tdidBytes(12) = ((leastSigBits >> 24) & 0xFF).asInstanceOf[Byte]
    tdidBytes(13) = ((leastSigBits >> 16) & 0xFF).asInstanceOf[Byte]
    tdidBytes(14) = ((leastSigBits >> 8 ) & 0xFF).asInstanceOf[Byte]
    tdidBytes(15) = ((leastSigBits >> 0 ) & 0xFF).asInstanceOf[Byte]

    val total = (
      toInt32(tdidBytes, 0).asInstanceOf[Long]
        + toInt32(tdidBytes, 4)
        + toInt32(tdidBytes, 8)
        + toInt32(tdidBytes, 12)
      )

    math.abs(total % basePopulation) < samplePopulation
  }

  def _isDeviceIdSampled(deviceId: String) : Boolean = {

    if (deviceId == "") {
      return false
    }

    // Parse GUID
    val guidAsLongs = guidToLongs(deviceId)

    // Must match Constants.UserIsSampled in AdPlatform.
    // Also must stay constant between runs in prod as we read datasets produced from multiple runs.
    userIsSampled(guidAsLongs.MostSigBits, guidAsLongs.LeastSigBits, 1000000, 10000)
  }

  val isDeviceIdSampled: UserDefinedFunction = udf(_isDeviceIdSampled _)
  val decodeTdid: UserDefinedFunction = udf(guidToLongs _)

  /////
  def runETLPipeline(): Unit = {
//    val df_seed_list = spark.read.format("parquet").load(seed_list_path)
//    val seeds = df_seed_list.collect()
//    val SeedIds: Array[String] = seeds(0).getSeq(0).toArray[String]
//    val SyntheticIds: Array[Int] = seeds(0).getSeq(1).toArray[Int]
//    val maxSyntheticId:Int = SyntheticIds.max
//    val SyntheticId2Idx: Array[Int] = new Array[Int](maxSyntheticId + 1)
//    SyntheticIds.zipWithIndex.foreach{case (id, idx) => SyntheticId2Idx(id) = idx} //.map{ case (sid, idx) => (sid, idx)}.toMap
    //val SensitiveSytheticIds: Array[Int] = seeds(0).getSeq(3).toArray[Int]
    //val dfSeed2SyntheticId = SeedIds.zip(SyntheticIds).toList.toDF("SeedId", "syntheticId")
    //val df_tdid_emb = if (tdid_limit > 1) spark.read.format("parquet").load(tdid_emb_path).limit(tdid_limit) else spark.read.format("parquet").load(tdid_emb_path)
    val df_seed_emb = spark.read.format("parquet").load(seed_emb_path)
    val df_density_features = new Array[DataFrame](10)
    for (i <- 0 until 10) {
      df_density_features(i) = if (density_limit > 1) spark.read.format("parquet").load(density_feature_path + s"split=${i}/").limit(density_limit) else spark.read.format("parquet").load(density_feature_path + s"split=${i}/")
    }
    val df_tdid_embs = new Array[DataFrame](10)
    for (i <- 0 until 10) {
      df_tdid_embs(i) = if (tdid_limit > 1) spark.read.format("parquet").load(tdid_emb_path + s"split=${i}/").limit(tdid_limit) else spark.read.format("parquet").load(tdid_emb_path + s"split=${i}/")
    }

    val df_sensitive_synthetic_ids = spark.read.parquet(policy_table_path)
      .where('Source === lit(DataSource.Seed.id) && 'CrossDeviceVendorId === lit(CrossDeviceVendor.None.id))
      .select('SourceId.as("SeedId"), 'SyntheticId.cast(IntegerType).as("SyntheticId"), 'IsSensitive)
    //val df_synthetic_ids = df_seed_list.select(explode('SyntheticIds).alias("SyntheticId"))

    val seedEmb = df_seed_emb.withColumn("SyntheticId", 'SyntheticId.cast(IntegerType)).join(df_sensitive_synthetic_ids, Seq("SyntheticId"), "inner")
      //.join(dfSeed2SyntheticId, Seq("SyntheticId"), "inner")
      .select('SeedId, 'SyntheticId, 'IsSensitive, 'Embedding) //"SeedId",
      .as[SyntheticEmbedding]
      .collect()

    val seedIds = seedEmb.map(_.SeedId)
    println(seedIds.mkString(","))

    val df = Seq(seedIds).toDF("SeedId")

    df.write.mode(SaveMode.Overwrite).parquet(seed_id_path)

    val df_seed_emb_sensitivity = spark.sparkContext.broadcast(
      seedEmb.map(e => (e.SyntheticId, if(e.IsSensitive) e.Embedding.take(EmbeddingSize) else e.Embedding.drop(EmbeddingSize)))
        .toSeq
    )

    val r = 1e-8f
    val smooth_factor = 30f
    val loc_factor = 0.8f
//      .withColumn("r", lit(1E-8))
//      .withColumn("weights",('r + (lit(1)-'r)*(lit(1)- lit(1)/(lit(1)+exp(-(lit(-smooth_factor)*('score-lit(loc_factor))))))))
//      .withColumn("relevance", 'score * 'weights)

    val relevanceScoreUDF = udf(
      (bdSenEmb: Array[Float], bdNonSenEmb: Array[Float], SyntheticIdsLevel1: Array[Int], SyntheticIdsLevel2: Array[Int], SeedSyntheticIdsLevel1: Array[Int], SeedSyntheticIdsLevel2: Array[Int]) => {
        val syntheticIdToLevel = mutable.HashMap[Int, Int]()
        if (SyntheticIdsLevel2.nonEmpty) SyntheticIdsLevel2.foreach(e => if (!syntheticIdToLevel.contains(e)) syntheticIdToLevel.put(e, 2 * EmbeddingSize))
        if (SeedSyntheticIdsLevel2.nonEmpty) SeedSyntheticIdsLevel2.foreach(e => if (!syntheticIdToLevel.contains(e)) syntheticIdToLevel.put(e, 2 * EmbeddingSize))

        if (SyntheticIdsLevel1.nonEmpty) SyntheticIdsLevel1.foreach(e => syntheticIdToLevel.put(e, 1 * EmbeddingSize))
        if (SeedSyntheticIdsLevel1.nonEmpty) SeedSyntheticIdsLevel1.foreach(e => syntheticIdToLevel.put(e, 1 * EmbeddingSize))
        df_seed_emb_sensitivity
          .value
          .map(
            e => {
              val score = if (e._2.length == EmbeddingSize)
                embeddings2scores(bdSenEmb, e._2, 0)
              else {
                val offset = syntheticIdToLevel.getOrElse(e._1, 0)
                embeddings2scores(bdNonSenEmb, e._2, offset)
              }
              val weight = r + (1 - r) * (1 - 1 / (1 + math.exp(-(-smooth_factor * (score - loc_factor)))))
              (weight * score).floatValue()
            }
          )
      }
    )

//    if (debug) {
//      print(s"df_final.rdd.getNumPartitions=${df_seed_emb_sensitivity.rdd.getNumPartitions}")
//    }

    val seedDensityFeature = spark.read.parquet(seed_density_path)
      .where(size(col("SyntheticIdLevel1")) =!= 0 || size(col("SyntheticIdLevel2")) =!= 0)
      .repartition(20480, 'SiteZipHashed)
    .select('SiteZipHashed, 'SyntheticIdLevel1, 'SyntheticIdLevel2)
      .cache()

    val topSiteZipHashed = broadcast(spark
      .read
      .format("tfrecord")
      .option("recordType", "Example")
      .load(avail_path)
      .groupBy('SiteZipHashed).agg(count("*").as("cnt")).orderBy('cnt.desc).limit(1000)
      .select('SiteZipHashed)
      .join(seedDensityFeature, Seq("SiteZipHashed"), "left")
    ).cache()

    (0 to 9).filter(density_split < 0 || _ == density_split).foreach(i => {
      //val i = 1
      print(f"Process split ${i}")

      // without 'TDID
      val dataset_prepare_to_calculate = df_tdid_embs(i).select('Uiid.as("TDID"), 'cnt, 'sen_pred_avg.cast(ArrayType(FloatType)).as("bdSenEmb"), 'non_sen_pred_avg.cast(ArrayType(FloatType)).as("bdNonSenEmb"), 'SiteZipHashed)

      df_density_features(i) = df_density_features(i)
        .filter(isDeviceIdSampled('TDID))
        .select("TDID", "SyntheticId_Level1", "SyntheticId_Level2")

      val df_final_with_seed_density = dataset_prepare_to_calculate
        .join(
          df_density_features(i), Seq("TDID"), "left"
        )

      //:- Project [SiteZipHashed#162, TDID#529, cnt#159L, bdSenEmb#530, bdNonSenEmb#531, SyntheticId_Level1#41, SyntheticId_Level2#39, SyntheticIdLevel1#352, SyntheticIdLevel2#353]
      val df_final =
        df_final_with_seed_density
          .join(topSiteZipHashed.select('SiteZipHashed), Seq("SiteZipHashed"), "left_anti")
        .repartition(20480, 'SiteZipHashed)
        .join(
          seedDensityFeature, Seq("SiteZipHashed"), "left"
        ).union(
            df_final_with_seed_density
              .join(topSiteZipHashed, Seq("SiteZipHashed"), "inner")
          )
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
  }
}

case class SyntheticEmbedding(SeedId: String, SyntheticId: Int, IsSensitive: Boolean, Embedding: Array[Float])