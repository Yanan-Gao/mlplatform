package job

import com.thetradedesk.kongming._
import com.thetradedesk.kongming.datasets._
import com.thetradedesk.kongming.datasets.TrainSetFeatureMappingDataset
import com.thetradedesk.kongming.transform.TrainSetFeatureMappingTransform

import java.time.LocalDate

object GenerateTrainSetFeatureMapping extends KongmingBaseJob {

  val fixedDateParquet: LocalDate = LocalDate.of(2022, 11,15)

  override def jobName: String = "GenerateTrainSetFeatureMapping"

  override def runTransform(args: Array[String]): Array[(String, Long)] = {

    val bidsImpressions = DailyBidsImpressionsDataset().readDate(date)
    val featureMappings = TrainSetFeatureMappingTransform.dailyTransform(date, bidsImpressions)(getPrometheus)
    val featureMappingRows = TrainSetFeatureMappingDataset().writePartition(featureMappings, fixedDateParquet, Some(100))

    Array(featureMappingRows)
  }


}
