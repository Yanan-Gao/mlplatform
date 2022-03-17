package com.ttd.mycellium.vertica

import com.ttd.features.datasets.{BidRequest, ReadableDataFrame}
import com.ttd.features.transformers._
import com.ttd.features.{Feature, FeatureConfig, FeatureDriver}
import com.ttd.mycellium.Vertex.UserID
import com.ttd.mycellium.spark.TTDSparkContext.spark
import com.ttd.mycellium.spark.config.TTDConfig.config
import org.apache.spark.ml.{Pipeline, PipelineModel}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.{FloatType, IntegerType, MapType, StringType}
import org.apache.spark.sql.{DataFrame, SaveMode}
import org.joda.time.DateTime

class IDConfig extends FeatureConfig {
  val fileCount: Int = config.getInt("fileCount", 4)
  val outputDate: DateTime = DateTime.parse(config.getString("outputDate", DateTime.now.toString))

  val dateFormat = "yyyyMMdd"
  val bidRequest: ReadableDataFrame = BidRequest()
  override def sources: Map[String, Seq[DateTime]] = Map(
    "provisioning2.AdGroup" -> Seq(outputDate)
  )
  val saveMode: SaveMode = SaveMode.Overwrite
  val outputLocation: String = config.getString("outputLocation",
    s"s3://thetradedesk-useast-hadoop/Data_Science/christopher.hawkes/application/feature/mycellium/vertex/ID"
      + s"/date=${outputDate.toString(dateFormat)}"
  )
}

class IDDriver(override val config: IDConfig) extends Feature[IDConfig] {
  override val featureName: String = "ID"
  override def createPipeline: Pipeline = new IDPipeline()

  override def getPipelineInput: DataFrame = {
    config.bidRequest.read(Seq(config.outputDate))(spark)
  }

  override def writePipelineOutput(dataFrame: DataFrame, pipelineModel: PipelineModel): Unit = {
    dataFrame.write.mode(config.saveMode).parquet(config.outputLocation)
  }
}

object IDDriver extends FeatureDriver[IDDriver] {
  override val driver: IDDriver = new IDDriver(new IDConfig)
}

class IDPipeline extends Pipeline {
  setStages(Array(
    Filter(col("TDID") =!= "00000000-0000-0000-0000-000000000000"),
    Select("LogEntryTime", "IPAddress", "DeviceType", "TDID", "OperatingSystem", "Browser",
      "OperatingSystemFamily", "DeviceAdvertisingId", "DeviceMake", "DeviceModel"),
    DropNA(Array("TDID", "DeviceAdvertisingId"), "all"),
    Distinct(),
    Select(Seq(
      unix_timestamp(col("LogEntryTime")) as "ts",
      UserID.vType as "v_type",
      UserID.idColumn as "v_id",
      map(
        lit("Browser"), col("Browser.value"),
        lit("DeviceType"), col("DeviceType.value"),
        lit("DeviceModel"), col("DeviceModel"),
        lit("OperatingSystem"), col("OperatingSystem.value"),
        lit("OperatingSystemFamily"), col("OperatingSystemFamily.value"),
      ) as "intProps",
      map(
        lit("TDID"), col("TDID"),
        lit("IPAddress"), col("IPAddress"),
        lit("DeviceAdvertisingId"), col("DeviceAdvertisingId")
      ) as "stringProps",
    )),
    WithEmptyMap("floatProps", MapType(StringType, FloatType)),
  ))
}