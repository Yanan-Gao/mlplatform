package com.ttd.mycellium.spark

import TTDSparkContext.spark
import org.apache.spark.sql.DataFrame

object VerticaConnectorUtil {
  def read(opts: Map[String, String]): DataFrame = {
    spark
      .read
      .format("com.vertica.spark.datasource.VerticaSource")
      .options(opts)
      .load()
  }

  def read(query: String,
           user: String,
           password: String,
           stagingFSUrl: String = "s3a://thetradedesk-useast-hadoop/Data_Science/christopher.hawkes/tmp",
           accessKey: String,
           secretKey: String,
           awsRegion: String = "us-east-1",
           db: String = "theTradeDesk",
           host: String = "vertdb.adsrvr.org",
           numPartitions: String = "4"
          ): DataFrame = {
    val opts = Map(
      "query" -> query,
      "host" -> host,
      "user" -> user,
      "db" -> db,
      "password" -> password,
      "staging_fs_url" -> stagingFSUrl,
      "query" -> "SELECT ZipId, CountryId, Name, VerticaWatermark, max_mark FROM provisioning2.zip LIMIT 10",
      "aws_access_key_id" -> accessKey,
      "aws_secret_access_key" -> secretKey,
      "aws_region" -> awsRegion,
      "num_partitions" -> numPartitions
    )
    read(opts)
  }
}
