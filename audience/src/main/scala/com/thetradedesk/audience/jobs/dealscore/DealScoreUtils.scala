package com.thetradedesk.audience.jobs.dealscore

import com.thetradedesk.audience.jobs.modelinput.rsmv2.usersampling.SIBSampler._isDeviceIdSampledNPercent
import com.thetradedesk.availspipeline.spark.datasets.IdentityAndDealAggHourlyDataSet
import com.thetradedesk.geronimo.shared.parquetDataPaths
import com.thetradedesk.spark.TTDSparkContext.spark
import com.thetradedesk.spark.TTDSparkContext.spark.implicits._
import org.apache.spark.sql.expressions.{UserDefinedFunction, Window}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{DataFrame, SparkSession}

import java.time.LocalDate

object DealScoreUtils {
  
  case class UserIdentifier(identitySource: Int, guidString: String)
  
  // Define the function to coalesce all the ID to get final TDID
  //TODO: create mapping for https://gitlab.adsrvr.org/thetradedesk/adplatform/-/blob/master/TTD/Domain/Shared/EnumsAndConstants/TTD.Domain.Shared.EnumsAndConstants.Identity/IdentitySource.cs
  //public enum IdentitySource
  //    {
  //        Unknown = 0,
  //        Tdid = 1,
  //        MiscDeviceId = 2,
  //        UnifiedId2 = 3, // when consuming this value, ensure there is equivalent handling for IdentitySource.EUID (or add a comment if explicitly excluding EUID)
  //        IdentityLinkId = 4,
  //        EUID = 5, // when consuming this value, ensure there is equivalent handling for IdentitySource.UnifiedId2 (or add a comment if explicitly excluding UnifiedId2)
  //        Nebula = 6,
  //        HashedIP = 7,
  //        CrossDevice = 8, // !!Important!! This value is deliberately not in dbo.IdentitySource in the database.
  //        // This is a fake value used when updating the adfrequency source used (see the function GetLoggedBitmapFromIdentitySources for example).
  //        DATId = 9,
  def makeProcessUserIdentifiersUDF(samplingRate: Int): UserDefinedFunction = {
    val f = (userIdentifiers: Seq[UserIdentifier]) => {
      val priorityOrder = Seq(1, 2, 3, 4, 5)
      userIdentifiers
        .filter(ui => priorityOrder.contains(ui.identitySource))
        .filter(ui => _isDeviceIdSampledNPercent(ui.guidString, samplingRate))
        .filter(ui => ui.guidString.length > 8 && ui.guidString.charAt(8) == '-')
        .map(_.guidString)
    }
    udf(f)
  }

  /**
   * Common function to load avails data with hour filtering
   */
  def loadAvailsData(dateStrDash: String, min_hour: Int, max_hour: Int): DataFrame = {
    val dayToRead = LocalDate.parse(dateStrDash).atStartOfDay()
    IdentityAndDealAggHourlyDataSet.readFullDayPartition(dayToRead).filter(col("hour") >= min_hour && col("hour") < max_hour)
  }

  /**
   * Common function to filter avails data and process user identifiers
   * @param avails raw avails DataFrame
   * @param samplingRate sampling rate for user identifier processing
   * @param selectColumns columns to select before processing (should include "UserIdentifiers")
   */
  def processUserIdentifiers(avails: DataFrame, samplingRate: Int, selectColumns: Seq[String]): DataFrame = {
    val baseFilter = expr("exists(UserIdentifiers, x -> x.identitySource IN (1,2,3,4,5))")
    
    val availsFiltered = avails.filter(baseFilter).select(selectColumns.map(col): _*)
    
    // Process user identifiers
    val processUserIdentifiersUDF = makeProcessUserIdentifiersUDF(samplingRate)
    val finalSelectColumns = ("Identifiers" +: selectColumns.filterNot(_ == "UserIdentifiers"))
    
    availsFiltered
      .withColumn("Identifiers", processUserIdentifiersUDF(col("UserIdentifiers")))
      .filter(size(col("Identifiers")) > 0)
      .select(finalSelectColumns.map(col): _*)
  }


  def getScoredTdids(tdidsByPropsPath: String, usersScoresPath: String, numberOfPartitions: Int,
                     capUsers: Int, date: LocalDate, lookback: Int, columns: Seq[String], keepScores: Boolean): DataFrame = {

    val paths = parquetDataPaths(tdidsByPropsPath, date, None, Some(lookback))
    val TDIDByProps = spark.read.option("basePath", tdidsByPropsPath).parquet(paths: _*).select(((columns :+ "TDID").map(col): _*)).distinct().repartition(numberOfPartitions, col("TDID"))
    val user_scores = spark.read.format("parquet").load(usersScoresPath).select(col("TDID"), col("Score")).repartition(numberOfPartitions, col("TDID"))

    // check if the tdid has a score. Bring in the score later after heavy operations
    val scoredTdids = {
      val baseDF = TDIDByProps
        .join(user_scores, Seq("TDID"))
        .select(
          ((columns).map(col) :+ TDIDByProps("TDID") :+ user_scores("Score")): _*
        )

      if (!keepScores) baseDF.drop("Score") else baseDF
    }
    scoredTdids
  }


  implicit class DataFrameOps(df: DataFrame) {

    def joinWithSeedData(
                          seedsToScore: DataFrame,
                          partitionColumns: Seq[String],
                          hasScoreColumn: Boolean = false,
                          broadcastSeedData: Boolean = false
                        )(implicit spark: SparkSession): DataFrame = {

      val seedDataToJoin = if (broadcastSeedData) broadcast(seedsToScore) else seedsToScore
      val baseColumns = partitionColumns :+ "TDID" :+ "SeedId"
      val selectColumns = if (hasScoreColumn) baseColumns :+ "Score" else baseColumns

      df.join(seedDataToJoin, partitionColumns)
        .select(selectColumns.map(col): _*)
    }

    def joinWithUserScore(
                           partitionColumns: Seq[String],
                           usersScoresPath: String,
                           numberOfPartitions: Int,
                         ): DataFrame = {
      val userScores = spark.read.format("parquet").load(usersScoresPath)
      val baseColumns = partitionColumns :+ "TDID" :+ "SeedId"
      df
        .repartition(numberOfPartitions, col("TDID"))
        .join(userScores.repartition(numberOfPartitions, col("TDID")).as("user_scores_all"), Seq("TDID"))
        .select((baseColumns.map(col) :+ $"user_scores_all.Score"): _*)

    }

    def extractSeedScore(
                          partitionColumns: Seq[String],
                          seedIdPath: String,
                        ): DataFrame = {

      val seedIds = spark.read.format("parquet").load(seedIdPath)
      val posexplodedSeeds = seedIds.select(posexplode(col("SeedId")).as(Seq("pos", "SeedId")))
      val finalDf = df
        .join(broadcast(posexplodedSeeds), Seq("SeedId"))
        .withColumn("Score", element_at(col("Score"), col("pos") + 1))
        .select((partitionColumns :+ "TDID" :+ "SeedId" :+ "Score").map(col): _*)

      finalDf.withColumn("tdidValueStruct", struct(col("TDID"), col("Score")))
    }

    def capScoredUsers(
                        numberOfPartitions: Int,
                        capUsers: Int,
                        columns: Seq[String],
                        seedScorePresent: Boolean
                      ): DataFrame = {
      val partitionCols = if (seedScorePresent) columns :+ "SeedId" else columns
      val windowSpec = Window.partitionBy(partitionCols.map(col): _*).orderBy(col("rand"))

      df.repartition(numberOfPartitions, partitionCols.map(col): _*)
        .withColumn("rand", rand())
        .withColumn("row_num", row_number().over(windowSpec))
        .filter(col("row_num") <= capUsers)
        .drop("row_num")
        .drop("rand")
    }

    def collectScores(
                       partitionColumns: Seq[String],
                       numberOfPartitions: Int,
                     ): DataFrame = {

      df.repartition(numberOfPartitions, (partitionColumns :+ "SeedId").map(col): _*)
        .groupBy((partitionColumns :+ "SeedId").map(col): _*)
        .agg(collect_list(col("tdidValueStruct")).as("Scores"))
    }
  }
  /**
   * Common utility for the "explode -> select -> join -> distinct" pattern used in Find jobs
   * 
   * @param sourceDF The input DataFrame to process
   * @param explodeColumns Map of source column to target column for explode operations
   * @param selectAfterExplode Columns to select after all explode operations
   * @param joinDF DataFrame to join with (will be broadcasted)
   * @param joinColumns Columns to join on
   * @return Processed DataFrame with exploded, joined, and deduplicated data
   */
  def explodeJoinAndDistinct(
    sourceDF: DataFrame,
    explodeColumns: Map[String, String], // sourceCol -> targetCol
    selectAfterExplode: Seq[String],
    joinDF: DataFrame,
    joinColumns: Seq[String]
  ): DataFrame = {

    // Step 1: Apply all explode operations
    val explodedDF = explodeColumns.foldLeft(sourceDF) { case (df, (sourceCol, targetCol)) =>
      df.withColumn(targetCol, explode(col(sourceCol)))
    }

    // Step 2: Select, join, and distinct
    explodedDF
      .join(broadcast(joinDF), joinColumns)
      .select(selectAfterExplode.map(col): _*)
      .distinct()
  }

  /**
   * Limit users per partition group and save to parquet
   * 
   * @param sourceDF The DataFrame to limit and save
   * @param partitionColumns Columns to partition/limit by (e.g., Seq("DealCode", "SupplyVendorId"))
   * @param numberOfPartitions Number of partitions for repartitioning
   * @param limitUsers Maximum users per partition group
   * @param coalesceTo Number of final partitions before writing
   * @param outPath Output path for parquet files
   */
  def limitAndSave(
    sourceDF: DataFrame,
    partitionColumns: Seq[String],
    numberOfPartitions: Int,
    limitUsers: Int,
    coalesceTo: Int,
    outPath: String
  ): Unit = {

    val windowSpec = Window.partitionBy(partitionColumns.map(col): _*).orderBy(col("rand"))

    sourceDF
      .repartition(numberOfPartitions, partitionColumns.map(col): _*)
      .withColumn("rand", rand())
      .withColumn("row_num", row_number().over(windowSpec))
      .filter(col("row_num") <= limitUsers)
      .drop("row_num")
      .drop("rand")
      .coalesce(coalesceTo)
      .write
      .format("parquet")
      .mode("overwrite")
      .option("codec", "org.apache.hadoop.io.compress.GzipCodec")
      .save(outPath)
  }
}
