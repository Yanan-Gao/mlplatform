package com.ttd.mycellium.drivers

import com.ttd.features.FeatureConfig
import com.ttd.features.datasets.{BidFeedback, ReadableDataFrame}
import com.ttd.features.util.PipelineUtils._
import com.ttd.mycellium.pipelines.bidfeedback.{BidFeedbackInputPipe, ServesEdgesPipe}
import com.ttd.mycellium.spark.TTDSparkContext.spark
import com.ttd.mycellium.spark.config.TTDConfig.config
import org.apache.spark.sql.SaveMode
import org.joda.time.DateTime

class BidFeedbackConfig extends FeatureConfig {
  val outputDate: DateTime = DateTime.parse(config.getString("outputDate", DateTime.now.toString))

  val dateFormat = "yyyyMMdd"
  val bidFeedback: ReadableDataFrame = BidFeedback()
  override def sources: Map[String, Seq[DateTime]] = Map(
    bidFeedback.basePath -> Seq(outputDate)
  )
  val saveMode: SaveMode = SaveMode.Overwrite

  val outputBasePath = "s3://thetradedesk-useast-hadoop/Data_Science/christopher.hawkes/application/feature/mycellium"
}

class BidFeedbackDriver(val config: BidFeedbackConfig) extends Runnable {
  override def run(): Unit = {
    val br = config.bidFeedback.read(Seq(config.outputDate))(spark)

    val bidFeedbackInputPipe = new BidFeedbackInputPipe()
    val dataFrame = bidFeedbackInputPipe.fitTransform(br)

    // edge pipes
    val trackedAtEdgesPipe = new ServesEdgesPipe()

    // write edges
    trackedAtEdgesPipe.fitTransform(dataFrame)
      .write.mode(config.saveMode).parquet(config.outputBasePath
      + "/edge/type=Serves"
      + s"/date=${config.outputDate.toString(config.dateFormat)}")
  }
}

object BidFeedbackDriver {
  def main(args: Array[String]): Unit = {
    new BidFeedbackDriver(new BidFeedbackConfig).run()
  }
}

