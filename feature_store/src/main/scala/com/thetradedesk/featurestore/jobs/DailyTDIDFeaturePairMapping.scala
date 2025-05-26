package com.thetradedesk.featurestore.jobs

import com.thetradedesk.featurestore._
import org.apache.spark.sql.SaveMode

object DailyTDIDFeaturePairMapping extends DensityFeatureBaseJob {
  override val jobName: String = "DailyTDIDFeaturePairMapping"
  val repartitionNum: Int = 64

  override def runTransform(args: Array[String]): Unit = {
    val bidreq = readBidsImpressionsWithIDExploded(featurePairs, date, None)

    featurePairStrings.foreach { featurePair =>
      val writePath = s"$MLPlatformS3Root/$ttdEnv/profiles/source=bidsimpression/index=TDID/job=$jobName/config=${featurePair}/v=1/date=${getDateStr(date)}/"
      bidreq.select("TDID", s"${featurePair}Hashed")
        .na.drop()
        .distinct()
        .repartition(repartitionNum)
        .write.mode(SaveMode.Overwrite)
        .parquet(writePath)
    }
  }
}
