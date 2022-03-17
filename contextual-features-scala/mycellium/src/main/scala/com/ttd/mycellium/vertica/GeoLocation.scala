package com.ttd.mycellium.vertica

import com.ttd.features.{Feature, FeatureConfig, FeatureDriver}
import com.ttd.mycellium.spark.VerticaConnectorUtil
import com.ttd.mycellium.spark.config.TTDConfig.config
import com.ttd.mycellium.util.{S3AccessConfig, VerticaAccessConfig}
import org.apache.spark.ml.{Pipeline, PipelineModel, PipelineStage}
import org.apache.spark.sql.{DataFrame, SaveMode}
import org.joda.time.DateTime

class GeoLocationConfig extends FeatureConfig with VerticaAccessConfig with S3AccessConfig {
  val fileCount: Int = config.getInt("fileCount", 4)
  val outputDate: DateTime = DateTime.parse(config.getString("outputDate", DateTime.now.toString))

  val verticaQuery: String = config.getString("verticaQuery",
    s"""WITH Zips as (
      |    SELECT ZipId, CountryId, Name, VerticaWatermark, max_mark
      |    FROM provisioning2.zip
      |    WHERE VerticaWatermark > '${outputDate.toString(verticaDateFormat)}'
      |), Geos as (
      |    SELECT CountryId, ShortName, LongName, Iso3, VerticaWatermark
      |    FROM provisioning2.country
      |    WHERE VerticaWatermark > '${outputDate.toString(verticaDateFormat)}'
      |)
      |    SELECT z.ZipId, z.CountryId, z.Name, g.ShortName, g.LongName, g.Iso3
      |    FROM Zips z left join Geos g on z.CountryId = g.CountryId
      |""".stripMargin
  )

  override def sources: Map[String, Seq[DateTime]] = Map(
    "provisioning2.zip" -> Seq(outputDate),
    "provisioning2.country" -> Seq(outputDate)
  )
  val saveMode: SaveMode = SaveMode.Overwrite
  val outputLocation: String = config.getString("outputLocation",
    s"s3://thetradedesk-useast-hadoop/Data_Science/christopher.hawkes/application/feature/mycellium/geolocation"
      + s"/date=${outputDate.toString(verticaDateFormat)}"
  )
}

class GeoLocationDriver(override val config: GeoLocationConfig) extends Feature[GeoLocationConfig] {
  override val featureName: String = "GeoLocation"
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

object GeoLocationDriver extends FeatureDriver[GeoLocationDriver] {
  override val driver: GeoLocationDriver = new GeoLocationDriver(new GeoLocationConfig)
}
