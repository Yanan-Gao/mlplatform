package com.ttd.mycellium.vertica

import com.ttd.features.{Feature, FeatureConfig, FeatureDriver}
import com.ttd.mycellium.spark.VerticaConnectorUtil
import com.ttd.mycellium.spark.config.TTDConfig.config
import com.ttd.mycellium.util.{S3AccessConfig, VerticaAccessConfig}
import org.apache.spark.ml.{Pipeline, PipelineModel, PipelineStage}
import org.apache.spark.sql.{DataFrame, SaveMode}
import org.joda.time.DateTime

class TrackingTagConfig extends FeatureConfig with VerticaAccessConfig with S3AccessConfig {
  val fileCount: Int = config.getInt("fileCount", 4)
  val outputDate: DateTime = DateTime.parse(config.getString("outputDate", DateTime.now.toString))

  val verticaQuery: String = config.getString("verticaQuery",
    s"""SELECT * FROM provisioning2.TrackingTag
      | WHERE VerticaWatermark > '${outputDate.toString(verticaDateFormat)}'
      |""".stripMargin
  )

  override def sources: Map[String, Seq[DateTime]] = Map(
    "provisioning2.TrackingTag" -> Seq(outputDate)
  )
  val saveMode: SaveMode = SaveMode.Overwrite
  val outputLocation: String = config.getString("outputLocation",
    s"s3://thetradedesk-useast-hadoop/Data_Science/christopher.hawkes/application/feature/mycellium/trackingtag"
      + s"/date=${outputDate.toString(verticaDateFormat)}"
  )
}

class TrackingTagDriver(override val config: TrackingTagConfig) extends Feature[TrackingTagConfig] {
  override val featureName: String = "TrackingTag"
  override def createPipeline: Pipeline = new Pipeline().setStages(Array.empty[PipelineStage])

  override def getPipelineInput: DataFrame = {
    VerticaConnectorUtil.read(
      query = config.verticaQuery,
      user = config.user,
      password = config.password,
      accessKey = config.accessKey,
      secretKey = config.secretKey,
      numPartitions = config.fileCount.toString
    )
  }

  override def writePipelineOutput(dataFrame: DataFrame, pipelineModel: PipelineModel): Unit = {
    dataFrame.write.mode(config.saveMode).parquet(config.outputLocation)
  }
}

object TrackingTagDriver extends FeatureDriver[TrackingTagDriver] {
  override val driver: TrackingTagDriver = new TrackingTagDriver(new TrackingTagConfig)
}
