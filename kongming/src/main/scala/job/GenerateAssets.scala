package job

import com.thetradedesk.kongming._
import com.thetradedesk.kongming.datasets.{DailyBidsImpressionsDataset, ImpressionPlacementIdDataset}
import com.thetradedesk.kongming.transform.AssetsTransform
import com.thetradedesk.spark.TTDSparkContext.spark
import com.thetradedesk.spark.util.TTDConfig.config
import com.thetradedesk.spark.util.prometheus.PrometheusClient


object GenerateAssets extends KongmingBaseJob {
  override def jobName: String = "GenerateAssetsTable"

  override def runTransform(args: Array[String]): Array[(String, Long)] = {

    // input params
    val lookbackDays = config.getInt(path="assetsLookback", 15)

    val bidsImpressions = DailyBidsImpressionsDataset().readRange(date.minusDays(lookbackDays), date, isInclusive = true)

    // generate top placement id as model assets
    val impressionPlacementDF = AssetsTransform.topPlacementPerSite(bidsImpressions)(getPrometheus)
    val impressionPlacementIdRows = ImpressionPlacementIdDataset().writePartition(impressionPlacementDF, date, Some(1))

    Array(impressionPlacementIdRows)

  }
}
