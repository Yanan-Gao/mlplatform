package job


import com.thetradedesk.geronimo.bidsimpression.schema.BidsImpressions
import com.thetradedesk.geronimo.bidsimpression.schema.BidsImpressionsSchema
import com.thetradedesk.geronimo.shared.GERONIMO_DATA_SOURCE
import com.thetradedesk.geronimo.shared.loadParquetData
import com.thetradedesk.kongming
import com.thetradedesk.kongming.datasets.AdGroupDataset
import com.thetradedesk.kongming.datasets.DailyBidRequestDataset
import com.thetradedesk.kongming.datasets.DailyBidRequestRecord
import com.thetradedesk.kongming.datasets.DailyConversionDataRecord
import com.thetradedesk.kongming.datasets.DailyConversionDataset
import com.thetradedesk.kongming.datasets.DailyPositiveBidRequestDataset
import com.thetradedesk.logging.Logger
import com.thetradedesk.spark.TTDSparkContext.spark.implicits._
import com.thetradedesk.spark.util.TTDConfig.config
import com.thetradedesk.kongming.datasets.AdGroupPolicyDataset
import com.thetradedesk.kongming.datasets.AdGroupRecord
import com.thetradedesk.spark.util.prometheus.PrometheusClient
import com.thetradedesk.spark.TTDSparkContext.spark
import com.thetradedesk.kongming.date
import com.thetradedesk.kongming.policyDate
import com.thetradedesk.kongming.transform.PositiveLabelDailyTransform
import org.apache.spark.sql.functions._

import java.time.LocalDate

/**
 * object to join conversion data with last touches in bidrequest dataset.
 */
object PositiveLabelGenerator extends Logger{
  //TODO: ideally policy level lookback would be based on attribution window. This below value will set a cap.
  //TODO: based on research yuehan did: https://atlassian.thetradedesk.com/jira/browse/AUDAUTO-284 plus a buffer
  val bidLookback = config.getInt("bidLookback", default = 20)
  //TODO: longer conv lookback will add more white space. For now, we settle on daily processing.
  val convLookback = config.getInt("convLookback", default=1)

  def main(args: Array[String]): Unit = {

    //TODO: need to consider attributed event here? consider in an implicit way by taking into last imp into account
    //maybe we blend in a light translation layer here? for attributed events?
    //keep the last n touches on bidrequest and 1 touch on bidfeedback and 1 touch on click if click is there.
    //indicating long vs short window and use weight to differenciate them.

    //load config datasets
    val prometheus = new PrometheusClient("KoaV4Conversion", "PositiveLabeling")

    // read master policy
    val adGroupPolicyHardCodedDate = policyDate
    val adGroupPolicy = AdGroupPolicyDataset.readHardCodedDataset(adGroupPolicyHardCodedDate).cache
    val adGroupDS = loadParquetData[AdGroupRecord](AdGroupDataset.ADGROUPS3, kongming.date)

    // resolve for maxLookback
    val maxPolicyLookbackInDays = adGroupPolicy.agg(max($"DataLookBack")).head.getAs[Int](0)
    val lookback = math.min(maxPolicyLookbackInDays, bidLookback) - 1 //the -1 is to account for the given date is partial

    //previous multiday data
    val rawMultiDayBidRequestDS = loadParquetData[DailyBidRequestRecord](
      DailyBidRequestDataset.S3BasePath,
      date.minusDays(1),//substract one day here as well
      lookBack = Some(lookback))

    //single day data
    val bidImpressionsS3Path = BidsImpressions.BIDSIMPRESSIONSS3 + "prod/bidsimpressions/"
    val bidsImpressions = loadParquetData[BidsImpressionsSchema](bidImpressionsS3Path, date, source = Some(GERONIMO_DATA_SOURCE))

    val dailyConversionDS = loadParquetData[DailyConversionDataRecord](DailyConversionDataset.S3BasePath, date).cache
    val sameDayPositiveBidRequestDS = PositiveLabelDailyTransform.intraDayConverterNTouchesTransform(
      bidsImpressions
      , adGroupPolicy
      , dailyConversionDS
      , adGroupDS
    )(prometheus)

    //join conversion and unioned dataset to get the final result
    //Note: join conversion first and then do rank will speed up calculation.
    val multiDayPositiveBidRequestDS = PositiveLabelDailyTransform.multiDayConverterTransform(
      rawMultiDayBidRequestDS,
      dailyConversionDS,
      adGroupPolicy
    )(prometheus)

    //union same day bidrequest with previous many days
    val unionedPositiveBidRequestDS = sameDayPositiveBidRequestDS.union(multiDayPositiveBidRequestDS)

    val positiveLabelDS = PositiveLabelDailyTransform.positiveLabelAggTransform(unionedPositiveBidRequestDS, adGroupPolicy)

    DailyPositiveBidRequestDataset.writePartition(positiveLabelDS, date)
  }
}