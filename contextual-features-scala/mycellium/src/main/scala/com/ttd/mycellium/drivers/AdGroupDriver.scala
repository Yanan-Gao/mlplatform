package com.ttd.mycellium.drivers

import com.ttd.features.FeatureConfig
import com.ttd.features.util.PipelineUtils._
import com.ttd.mycellium.pipelines.adgroup.AdGroupVertexPipe
import com.ttd.mycellium.spark.VerticaConnectorUtil
import com.ttd.mycellium.spark.config.TTDConfig.config
import com.ttd.mycellium.util.{S3AccessConfig, VerticaAccessConfig}
import org.apache.spark.sql.SaveMode
import org.joda.time.DateTime

class AdGroupConfig extends FeatureConfig with VerticaAccessConfig with S3AccessConfig {
  val fileCount: Int = config.getInt("fileCount", 4)
  val outputDate: DateTime = DateTime.parse(config.getString("outputDate", DateTime.now.toString))
  val dateFormat = "yyyyMMdd"

  val verticaQuery: String = config.getString("verticaQuery",
    s"""SELECT AdGroupId, AdvertiserId, PartnerId, AdGroupTypeId, AdGroupName, AdGroupDescription, CampaignId, GeoSegmentId, CreatedAt, LastUpdatedAt, IsVideo, VerticaWatermark
      | FROM provisioning2.AdGroup
      | WHERE VerticaWatermark > '${outputDate.toString(verticaDateFormat)}'
      |""".stripMargin
  )

  override def sources: Map[String, Seq[DateTime]] = Map(
    "provisioning2.AdGroup" -> Seq(outputDate)
  )
  val saveMode: SaveMode = SaveMode.Overwrite

  val outputBasePath = "s3://thetradedesk-useast-hadoop/Data_Science/christopher.hawkes/application/feature/mycellium"
}

class AdGroupDriver(val config: AdGroupConfig) extends Runnable {
  override def run(): Unit = {
    val adgroupDF = VerticaConnectorUtil.read(
      query = config.verticaQuery,
      user = config.user,
      password = config.password,
      accessKey = config.accessKey,
      secretKey = config.secretKey,
      numPartitions = config.fileCount.toString
    )

    new AdGroupVertexPipe()
      .fitTransform(adgroupDF)
      .write.mode(config.saveMode)
      .parquet(config.outputBasePath
        + "/vertex/type=AdGroup"
        + s"/date=${config.outputDate.toString(config.dateFormat)}")
  }
}

object AdGroupDriver {
  def main(args: Array[String]): Unit = {
    new AdGroupDriver(new AdGroupConfig).run()
  }
}


