package com.ttd.mycellium.vertica

import com.ttd.features.{Feature, FeatureConfig, FeatureDriver}
import com.ttd.mycellium.spark.VerticaConnectorUtil
import com.ttd.mycellium.spark.config.TTDConfig.config
import com.ttd.mycellium.util.{S3AccessConfig, VerticaAccessConfig}
import org.apache.spark.ml.{Pipeline, PipelineModel, PipelineStage}
import org.apache.spark.sql.{DataFrame, SaveMode}
import org.joda.time.DateTime

class CreativeConfig extends FeatureConfig with VerticaAccessConfig with S3AccessConfig {
  val fileCount: Int = config.getInt("fileCount", 4)
  val outputDate: DateTime = DateTime.parse(config.getString("outputDate", DateTime.now.toString))

  val verticaQuery: String = config.getString("verticaQuery",
    s"""SELECT CreativeId, AdFormatId, AdvertiserId, ClickthroughUrl, Name, Description, CreatedAt, LastUpdatedAt, TagType, ClickTrackingSuccess, VerticaWatermark
      | FROM provisioning2.Creative
      | WHERE VerticaWatermark > '${outputDate.toString(verticaDateFormat)}'
      |""".stripMargin
  )

  override def sources: Map[String, Seq[DateTime]] = Map(
    "provisioning2.Creative" -> Seq(outputDate)
  )
  val saveMode: SaveMode = SaveMode.Overwrite
  val outputLocation: String = config.getString("outputLocation",
    s"s3://thetradedesk-useast-hadoop/Data_Science/christopher.hawkes/application/feature/mycellium/creative"
      + s"/date=${outputDate.toString(verticaDateFormat)}"
  )
}

class CreativeDriver(override val config: CreativeConfig) extends Feature[CreativeConfig] {
  override val featureName: String = "Creative"
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

object CreativeDriver extends FeatureDriver[CreativeDriver] {
  override val driver: CreativeDriver = new CreativeDriver(new CreativeConfig)
}
