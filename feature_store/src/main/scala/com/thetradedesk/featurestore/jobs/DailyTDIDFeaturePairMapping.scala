package com.thetradedesk.featurestore.jobs

import com.thetradedesk.featurestore._
import org.apache.spark.sql.SaveMode
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._

object DailyTDIDFeaturePairMapping extends DensityFeatureBaseJob {
  override val jobName: String = "DailyTDIDFeaturePairMapping"

  override def runTransform(args: Array[String]): Unit = {
    val bidreq = readBidsImpressionsWithIDExploded(featurePairs, date, None)

    featurePairStrings.foreach { featurePair =>
      val writePath = s"$MLPlatformS3Root/$writeEnv/profiles/source=bidsimpression/index=TDID/job=$jobName/config=${featurePair}/v=1/date=${getDateStr(date)}/"
      bidreq.select("TDID", s"${featurePair}Hashed")
        .na.drop()
        .groupBy("TDID", s"${featurePair}Hashed")
        .agg(count("*").as("FeatureFrequency"))
        .withColumn("UserFrequency", sum("FeatureFrequency").over(Window.partitionBy("TDID")))
        .write.mode(SaveMode.Overwrite)
        .parquet(writePath)
    }
  }
}
