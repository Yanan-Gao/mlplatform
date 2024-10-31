package com.thetradedesk.philo.transform

import com.thetradedesk.geronimo.bidsimpression.schema.BidsImpressionsSchema
import com.thetradedesk.geronimo.shared.{ARRAY_INT_FEATURE_TYPE, FLOAT_FEATURE_TYPE, INT_FEATURE_TYPE, STRING_FEATURE_TYPE, loadModelFeatures}
import com.thetradedesk.geronimo.shared.schemas.ModelFeature
import com.thetradedesk.logging.Logger
import com.thetradedesk.philo.{addOriginalCols, debugInfo, flattenData, schema, shiftModUdf}
import com.thetradedesk.philo.schema.{AdGroupDataSet, AdGroupRecord, AdvertiserExclusionRecord, CampaignROIGoalDataSet, CampaignROIGoalRecord, ClickTrackerRecord, CreativeLandingPageRecord, ModelInputRecord, ModelInputUserRecord}
import com.thetradedesk.spark.sql.SQLFunctions._
import org.apache.spark.sql.{Column, DataFrame, Dataset, SparkSession}
import org.apache.spark.sql.functions.{col, concat_ws, count, expr, lit, max, mean, size, stddev, sum, udf, when, xxhash64}
import com.thetradedesk.spark.TTDSparkContext.spark.implicits._
import job.CountryFilterRecord
import org.apache.spark.sql.expressions.UserDefinedFunction
import org.apache.spark.sql.types.IntegerType
import org.apache.spark.sql.expressions.Window
import com.thetradedesk.spark.util.TTDConfig.config

object ModelInputTransform extends Logger {

  val flatten_set = Set("AdsTxtSellerType","PublisherType", "DeviceType", "OperatingSystemFamily", "Browser", "RenderingContext", "DoNotTrack")
  val addCols = Seq("label", "BidRequestId", "UIID", "LogEntryTime", "excluded")
  // Precomputed thresholds for different number of impression percentiles used for click-bot filtering
  // can also use percentileToThreshold() to get exact values but this takes too much time
  val impression_97 = 18
  val impression_98 = 25
  val impression_99 = 40
  val impression_99_9 = 100
  val impression_99_99 = 250
  val maxImpressions = 1000


  // return training input based on bidimps combined dataset
  // if filterresults = true, it will be based on either AdGroupId (from s3 adgroup dataset) or from
  // Country (from s3 country filter dataset, or both
  // if it is landingpage, then it will be adgroup with CPC/CTR goal and adding landingpage
  // as the feature
  def transform(clicks: Dataset[ClickTrackerRecord],
                adgroup: Dataset[AdGroupRecord],
                bidsImpsDat: Dataset[BidsImpressionsSchema],
                filterAdGroup: Boolean,
                creativeLandingPage: Option[Dataset[CreativeLandingPageRecord]],
                countryFilter: Option[Dataset[CountryFilterRecord]],
                keptCols: Seq[String],
                modelFeatures: Seq[ModelFeature],
                addUserData: Boolean,
                filterClickBots: Boolean,
                numUserCols: Int,
                advertiserExclusionList: Option[Dataset[AdvertiserExclusionRecord]],
                debug: Boolean): (DataFrame, DataFrame) = {
    val (clickLabels, bidsImpsPreJoin) = addBidAndClickLabels(clicks, bidsImpsDat)
    val preFilteredData = preFilterJoin(clickLabels, bidsImpsPreJoin)
    val filteredData = preFilteredData
      .transform(ds => addExclusionFlag(ds, advertiserExclusionList))
      // if not filterResults, filterAdGroup will be false and countryFilter will be None, it will just left join adgroup
      .transform(ds => filterDataset(ds, adgroup, filterAdGroup, countryFilter))
      .transform(ds => creativeLandingPage.map(clp => ModelInputTransform.matchLandingPage(ds, clp)).getOrElse(ds))



    val (addKeptCols, originalColNames) = addOriginalCols(keptCols, filteredData.toDF)
    if (debug) {
      debugInfo("PreFilteredData", preFilteredData)
      debugInfo("filteredData", filteredData)
      debugInfo("addKeptCols", addKeptCols)
    }
    if (addUserData) {

      val data = addUserDataFeatures(addKeptCols, numUserCols, filterClickBots)
      val flatten = flattenData(data, flatten_set).selectAs[ModelInputUserRecord]
      val labelCounts = flatten.groupBy("label", "excluded").count()
      val hashedData = getHashedUserData(flatten, modelFeatures, originalColNames)

      val columnNames = (0 until numUserCols).map(i => s"UserData_Column$i")
      val updatedHashedData = hashedData.select(
        hashedData.columns.map {
          case colName if columnNames.contains(colName) =>
            when(col(colName) === 1, 0).otherwise(col(colName)).as(colName)
          case colName =>
            col(colName)
        }: _*
      )
      if (debug) {
        debugInfo("updatedHashedData", updatedHashedData)
      }
      (updatedHashedData, labelCounts)

  } else {

      val flatten = flattenData(addKeptCols, flatten_set).selectAs[ModelInputRecord]

      val labelCounts = flatten.groupBy("label", "excluded").count()
      val hashedData = getHashedData(flatten, modelFeatures, originalColNames)
      if (debug) {
        debugInfo("hashedData", hashedData)
      }
      (hashedData, labelCounts)
    }
  }

  def intModelFeaturesCols(inputColAndDims: Seq[ModelFeature]): Array[Column] = {
    inputColAndDims.map {
      case ModelFeature(name, STRING_FEATURE_TYPE, Some(cardinality), _) => when(col(name).isNotNullOrEmpty, shiftModUdf(xxhash64(col(name)), lit(cardinality))).otherwise(0).alias(name)
      case ModelFeature(name, INT_FEATURE_TYPE, Some(cardinality), _) => when(col(name).isNotNull, shiftModUdf(col(name), lit(cardinality))).otherwise(0).alias(name)
      case ModelFeature(name, FLOAT_FEATURE_TYPE, _, _) => col(name).alias(name)
    }.toArray
  }

  def addBidAndClickLabels(clicks: Dataset[ClickTrackerRecord],
                            bidsImpsDat: Dataset[BidsImpressionsSchema]) : (Dataset[ClickTrackerRecord], Dataset[BidsImpressionsSchema]) = {

    val clickLabels = clicks.withColumn("label", lit(1))
      .as[ClickTrackerRecord]

    val bidsImpsPreJoin = bidsImpsDat
      // is imp is a boolean
      .filter(col("IsImp"))
      .as[BidsImpressionsSchema]

    (clickLabels, bidsImpsPreJoin)
  }

  def filterDataset(preFilterDataset: DataFrame,
                    adgroup: Dataset[AdGroupRecord],
                    filterAdGroup: Boolean = false,
                    countryFilter: Option[Dataset[CountryFilterRecord]]
                   ): DataFrame = {
    preFilterDataset
      .transform(
        ds => // if filterAdGroup, adgroup is not just additional info, it is also used to filter adgroup for the training data set
        if (filterAdGroup) {ds.join(adgroup, Seq("AdGroupId", "CampaignId"), "inner")}
        else {ds.join(adgroup, Seq("AdGroupId", "CampaignId"), "leftouter")}
      )
      .transform(ds => countryFilter.map(filter => ds.join(filter, Seq("Country"))).getOrElse(ds))
  }


  def addUserDataFeatures(data: DataFrame, numUserCols: Int, filterClickBots: Boolean = false): DataFrame = {

    /*
    This function adds MatchedSegments, HasUserData, & UserDataLength
    --- MatchedSegments is an array of TargetingDataIds which is not supported by .csv
    --- Need to manually map every segment to its own "UserData_Column{i}"
     */

    // Filter data if removing click bots
    val filteredData = if (filterClickBots) {
      filterBots(data)
    } else {
      data
    }

    // Explode MatchedSegments into 170 individual columns
    val columnNames = (0 until numUserCols).map(i => s"UserData_Column$i")
    val updatedData = filteredData.select(
      col("*") +: (0 until numUserCols).map { i =>
        when(
          col("MatchedSegments").isNotNull && col("MatchedSegments").getItem(i).isNotNull,
          col("MatchedSegments").getItem(i).cast(IntegerType)
        ).otherwise(0).as(s"UserData_Column${i}")
      }: _*
    )

    // Add HasUserData column
    val dataWithHasUserData = updatedData.withColumn(
      "HasUserData",
      when(col("MatchedSegments").isNotNull && col("MatchedSegments").getItem(0).isNotNull, 1).otherwise(0)
    )

    // Add UserDataLength column
    val finalUserDataset = dataWithHasUserData.withColumn(
      "UserDataLength",
      when(col("UserSegmentCount").isNull, lit(0.0)).otherwise(col("UserSegmentCount") * lit(1.0))
    )

    // Return updated dataset with user data features
    finalUserDataset
  }

  def filterBots(preFilterDataset: DataFrame): DataFrame = {

    // Linear Interpolation Function that calculates interpolated CTR threshold between two points
    def lerp(a: Double, b: Double, t: Double): Double = {
      a + (b - a) * t
    }

    // Computes a dynamic CTR threshold based on the number of impressions and predefined control points.
    def dynamicCtrThreshold(
    numImpressions: Long,
    threshold97: Long,
    threshold98: Long,
    threshold99: Long,
    threshold999: Long,
    threshold9999: Long,
    controlPoints: Map[Double, Double],
    maxNumberOfImpressions: Long
    ): Double = {
      if (numImpressions >= threshold9999) {
        val t = (numImpressions - threshold9999).toDouble / (maxNumberOfImpressions - threshold9999)
        lerp(controlPoints(99.99), 0.30, t)
      } else if (numImpressions >= threshold999) {
        val t = (numImpressions - threshold999).toDouble / (threshold9999 - threshold999)
        lerp(controlPoints(99.9), controlPoints(99.99), t)
      } else if (numImpressions >= threshold99) {
        val t = (numImpressions - threshold99).toDouble / (threshold999 - threshold99)
        lerp(controlPoints(99), controlPoints(99.9), t)
      } else if (numImpressions >= threshold98) {
        val t = (numImpressions - threshold98).toDouble / (threshold99 - threshold98)
        lerp(controlPoints(98), controlPoints(99), t)
      } else if (numImpressions >= threshold97) {
        val t = (numImpressions - threshold97).toDouble / (threshold98 - threshold97)
        lerp(controlPoints(97), controlPoints(98), t)
      } else {
        1.01
      }
    }

    // Calculates the number of impressions corresponding to a specific percentile.
    def percentileToThreshold(dfTotalImpressionCounts: DataFrame, percentile: Double): Int = {
      val impression_threshold = dfTotalImpressionCounts
      .filter(col("CumulativeSum") > percentile)
      .orderBy(col("num_impressions"))
      .collect()(0).getAs[Long]("num_impressions").toInt

      impression_threshold
    }

    // Calculate the counts of label_0 (non-click) and label_1 (click) for each UIID
    val clickDataLabelCounts = preFilterDataset
      .groupBy("UIID").pivot("label").count().na.fill(0)
      .withColumnRenamed("0", "label_0_count")
      .withColumnRenamed("1", "label_1_count")
      .orderBy(col("label_1_count").desc)

    // Add a column for the total number of impressions per UIID
    val clickDataLabelCountsWithImpressions = clickDataLabelCounts.withColumn(
      "num_impressions",
      col("label_1_count") + col("label_0_count")
    )

    // Add a column for the CTR (Click-Through Rate) per UIID
    val clickDataLabelCountsFinal = clickDataLabelCountsWithImpressions.withColumn(
      "ctr",
      col("label_1_count") / col("num_impressions")
    )

    // Filter out invalid UIIDs (null or placeholder values)
    val dfValidClickData = clickDataLabelCountsFinal.filter(
      col("UIID").isNotNull && col("UIID") =!= "00000000-0000-0000-0000-000000000000"
    )

    val windowSpec = Window.orderBy("num_impressions").rowsBetween(Window.unboundedPreceding, Window.currentRow)

    // Calculate the cumulative sum and percentages for impression counts
    val dfTotalImpressionCounts = dfValidClickData
      .groupBy("num_impressions")
      .agg(count("*").alias("unique_UIIDs"))
      .orderBy(col("num_impressions").asc)
      .withColumn("percent", col("unique_UIIDs") / sum("unique_UIIDs").over(Window.partitionBy()))
      .withColumn("CumulativeSum", sum(col("percent")).over(windowSpec))

    // Define a set of control points that maps number of impression percentiles to CTR Rate
    // e.g. A user in the 97th percentile of number of impressions should have a CTR threshold of 0.90
    // while a user in the 99.99th percentile should have a CTR threshold of 0.30
    val controlPoints = Map(97.0 -> 0.90, 98.0 -> 0.75, 99.0 -> 0.60, 99.9 -> 0.50, 99.99 -> 0.30)

    val dynamicCtrThresholdUdf = udf((numImpressions: Long) =>
      dynamicCtrThreshold(numImpressions,
        impression_97,
        impression_98,
        impression_99,
        impression_99_9,
        impression_99_99,
        controlPoints,
        maxImpressions
      )
    )


    // Apply the UDF to create a dynamic CTR threshold column
    val dfWithDynamicThreshold = dfValidClickData.withColumn("ctr_threshold", dynamicCtrThresholdUdf(col("num_impressions")))

    // Identify outliers by making sure that the number of impressions is above the 97th percentile and that the CTR is above the dynamic CTR threshold
    val dfOutliers = dfWithDynamicThreshold.filter(
      (col("num_impressions") > impression_97) && (col("ctr") > col("ctr_threshold"))
    )

    // Get the distinct UIIDs that correspont to outliers/potential clickbots
    val clickbotUiids = dfOutliers
      .select("UIID")
      .distinct()

    // Remove the identified clickbot UIIDs from the original dataset
    val filteredDataWithoutBots = preFilterDataset.join(clickbotUiids, Seq("UIID"), "left_anti")

    filteredDataWithoutBots

  }

  def matchLandingPage(filteredData: DataFrame,
                       creativeLandingPage: Dataset[CreativeLandingPageRecord]): DataFrame = {
    val hashedData = filteredData.withColumn("hashedCreativeId", xxhash64(col("CreativeId")))
    val hashedCreativeLanding = creativeLandingPage.withColumn("hashedCreativeId", xxhash64(col("CreativeId"))).drop("CreativeID")
    hashedData.join(hashedCreativeLanding, Seq("hashedCreativeId")).drop("hashedCreativeId")
  }


  def preFilterJoin(clickLabels: Dataset[ClickTrackerRecord],
                    bidsImpsPreJoin: Dataset[BidsImpressionsSchema],
                    ): DataFrame = {
    bidsImpsPreJoin.join(clickLabels, Seq("BidRequestId"), "leftouter")
      .withColumn("label", when(col("label").isNull, 0).otherwise(1))
      .withColumn("AdFormat", concat_ws("x", col("AdWidthInPixels"), col("AdHeightInPixels")))
  }

  def getHashedData(flatten: Dataset[ModelInputRecord], modelFeatures: Seq[ModelFeature],
                    originalColNames: Seq[String]): DataFrame ={
    val origCols = addCols ++ originalColNames
    // todo: we need a better way to track these fields
    val selectionQuery = intModelFeaturesCols(modelFeatures) ++ origCols.map(col)

    flatten.select(selectionQuery: _*)

  }

  def getHashedUserData(flatten: Dataset[ModelInputUserRecord], modelFeatures: Seq[ModelFeature],
                    originalColNames: Seq[String]): DataFrame ={
    val origCols = addCols ++ originalColNames
    // todo: we need a better way to track these fields
    val selectionQuery = intModelFeaturesCols(modelFeatures) ++ origCols.map(col)

    flatten.select(selectionQuery: _*)

  }

  def addExclusionFlag(df: DataFrame, advertiserExclusionList: Option[Dataset[AdvertiserExclusionRecord]]
                      ): DataFrame = {
    // Check if the advertiser exclusion list is defined
    if (advertiserExclusionList.isDefined) {
      val exclusionList = advertiserExclusionList.get
      // Join with exclusion list and set the 'excluded' flag
      df.join(
        exclusionList.withColumn("excluded", lit(1)),
        Seq("AdvertiserId"),
        "leftouter"
      ).withColumn(
        "excluded", when(col("excluded").isNull, 0).otherwise(1)
      )
    } else {
      // If no exclusion list, mark all rows as not excluded (excluded = 0)
      df.withColumn("excluded", lit(0))
    }
  }

}
