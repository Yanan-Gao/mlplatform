package com.ttd.contextual.features.web.urlprofile

import com.ttd.contextual.datasets.sources.{IdentityAvails, ReadableDataFrame}
import com.ttd.contextual.util.elDoradoUtilities.spark.TTDConfig.config
import com.ttd.features.transformers.{Agg, DropNA, Filter, Select}
import com.ttd.features.{Feature, FeatureConfig}
import org.apache.spark.ml.{Pipeline, PipelineModel}
import org.apache.spark.sql.{DataFrame, SaveMode}
import org.apache.spark.sql.functions._
import org.joda.time.DateTime


class UrlHistoryDriverConfig extends FeatureConfig {
  // Yake extraction workflow parameters
  val maxDailyHistory: Int = config.getInt("maxDailyHistory", 1000)

  // Parallelism parameters
  val fileCount: Int = config.getInt("fileCount", 100)
  val tempDir: String = config.getString("tempDir", "hdfs:///user/hadoop/output-temp-dir")

  // Dataset parameters
  val avails: ReadableDataFrame = IdentityAvails()
  val availDateRanges: Seq[DateTime] = config.getStringSeq("dateRange", Seq()).map(DateTime.parse)

  // Column Parameters
  val urlCol: String = config.getString("urlCol", "Url")
  val uiidCol: String = config.getString("uiidCol", "uiid")
  val tsCol: String = config.getString("tsCol", "ts")

  val saveMode: SaveMode = SaveMode.Overwrite
  val outputLocation: String = config.getString("outputLocation",
    "s3://thetradedesk-useast-hadoop/Data_Science/christopher.hawkes/application/feature/url-history/")

  override def sources: Map[String, Seq[DateTime]] = Map(
    avails.basePath -> availDateRanges
  )
}


class UrlHistoryDriver(override val config: UrlHistoryDriverConfig) extends Feature[UrlHistoryDriverConfig] {
  override val featureName: String = "UrlHistory"

  override def createPipeline: Pipeline = new UrlHistoryPipeline(config)

  override def getPipelineInput: DataFrame = {
    config.avails.read(config.availDateRanges)
  }

  override def writePipelineOutput(dataFrame: DataFrame, pipelineModel: PipelineModel): Unit = {
    dataFrame.repartition(config.fileCount).write.mode(config.saveMode).parquet(config.outputLocation)
  }
}

class UrlHistoryPipeline(config: UrlHistoryDriverConfig) extends Pipeline {
  setStages(Array(
    Filter(col(config.uiidCol) =!= "00000000-0000-0000-0000-000000000000"),
    Select(Seq(col(config.uiidCol), col(config.urlCol).getItem(0) as "url", col("ts"))),
    DropNA(),
    Agg(
      Seq(col(config.uiidCol), col(config.urlCol)),
      Seq(max("ts") as "ts", count("*") as "count")
    ),
    Agg(
      Seq(col(config.uiidCol)),
      Seq(
        // take most frequent by count then by recency
        slice(
          sort_array(
            collect_list(
              struct(col("count"), col("ts"), col("url"))
            ),
            asc = false
          ),
          1, config.maxDailyHistory
        ) as "history"
      )
    ),
    // filter size 0 history?
    Filter(size(col("history")) > 0)
  ))
}