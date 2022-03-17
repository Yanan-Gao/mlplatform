package com.ttd.mycellium.drivers

import com.ttd.features.FeatureConfig
import com.ttd.features.datasets.{ConversionTracker, ReadableDataFrame}
import com.ttd.features.util.PipelineUtils._
import com.ttd.mycellium.pipelines.conversiontracker.{ConversionTrackerInputPipe, ConvertsEdgesPipe}
import com.ttd.mycellium.spark.TTDSparkContext.spark
import com.ttd.mycellium.spark.config.TTDConfig.config
import org.apache.spark.sql.SaveMode
import org.joda.time.DateTime

class ConversionTrackerConfig extends FeatureConfig {
  val outputDate: DateTime = DateTime.parse(config.getString("outputDate", DateTime.now.toString))

  val dateFormat = "yyyyMMdd"
  val conversionTracker: ReadableDataFrame = ConversionTracker()
  override def sources: Map[String, Seq[DateTime]] = Map(
    conversionTracker.basePath -> Seq(outputDate)
  )
  val saveMode: SaveMode = SaveMode.Overwrite

  val outputBasePath = "s3://thetradedesk-useast-hadoop/Data_Science/christopher.hawkes/application/feature/mycellium"
}

class ConversionTrackerDriver(val config: ConversionTrackerConfig) extends Runnable {
  override def run(): Unit = {
    val br = config.conversionTracker.read(Seq(config.outputDate))(spark)

    val bidFeedbackInputPipe = new ConversionTrackerInputPipe()
    val dataFrame = bidFeedbackInputPipe.fitTransform(br)

    // edge pipes
    val trackedAtEdgesPipe = new ConvertsEdgesPipe()

    // write edges
    trackedAtEdgesPipe.fitTransform(dataFrame)
      .write.mode(config.saveMode).parquet(config.outputBasePath
      + "/edge/type=Converts"
      + s"/date=${config.outputDate.toString(config.dateFormat)}")
  }
}

object ConversionTrackerDriver {
  def main(args: Array[String]): Unit = {
    new ConversionTrackerDriver(new ConversionTrackerConfig).run()
  }
}

