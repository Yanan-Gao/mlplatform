package com.ttd.mycellium.drivers

import com.ttd.features.FeatureConfig
import com.ttd.features.datasets.{ClickTracker, ReadableDataFrame}
import com.ttd.features.util.PipelineUtils._
import com.ttd.mycellium.pipelines.clicktracker.{ClickTrackerInputPipe, ClicksEdgesPipe}
import com.ttd.mycellium.spark.TTDSparkContext.spark
import com.ttd.mycellium.spark.config.TTDConfig.config
import org.apache.spark.sql.SaveMode
import org.joda.time.DateTime

class ClickTrackerConfig extends FeatureConfig {
  val outputDate: DateTime = DateTime.parse(config.getString("outputDate", DateTime.now.toString))

  val dateFormat = "yyyyMMdd"
  val clickTracker: ReadableDataFrame = ClickTracker()
  override def sources: Map[String, Seq[DateTime]] = Map(
    clickTracker.basePath -> Seq(outputDate)
  )
  val saveMode: SaveMode = SaveMode.Overwrite

  val outputBasePath = "s3://thetradedesk-useast-hadoop/Data_Science/christopher.hawkes/application/feature/mycellium"
}

class ClickTrackerDriver(val config: ClickTrackerConfig) extends Runnable {
  override def run(): Unit = {
    val br = config.clickTracker.read(Seq(config.outputDate))(spark)

    val clicksInputPipe = new ClickTrackerInputPipe()
    val dataFrame = clicksInputPipe.fitTransform(br)

    // edge pipes
    val trackedAtEdgesPipe = new ClicksEdgesPipe()

    // write edges
    trackedAtEdgesPipe.fitTransform(dataFrame)
      .write.mode(config.saveMode).parquet(config.outputBasePath
      + "/edge/type=Clicks"
      + s"/date=${config.outputDate.toString(config.dateFormat)}")
  }
}

object ClickTrackerDriver {
  def main(args: Array[String]): Unit = {
    new ClickTrackerDriver(new ClickTrackerConfig).run()
  }
}

