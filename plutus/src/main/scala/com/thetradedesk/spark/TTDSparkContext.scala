package com.thetradedesk.spark

import com.thetradedesk.logging.Logger
import org.apache.spark.sql.SparkSession

object TTDSparkContext extends Logger {
  private var isTestMode = false;

  def setTestMode(): Unit = {
    isTestMode = true
  }

  lazy val spark: SparkSession = getSpark()

  private def getSpark(): SparkSession = {
    val spark: SparkSession =
      if (isTestMode)
        SparkSession
          .builder()
          .config("fs.s3a.connection.maximum", 5000)
          .config("spark.master", "local")
          .config("spark.driver.bindAddress", "127.0.0.1")
          .config("spark.sql.shuffle.partitions", 4)
          .getOrCreate()
      else
        SparkSession
          .builder()
          .config("fs.s3a.connection.maximum", 5000)
          .getOrCreate()

    spark.sparkContext.hadoopConfiguration.set("mapreduce.fileoutputcommitter.algorithm.version", "2")
    spark.sqlContext.setConf("spark.sql.parquet.filterPushdown", "true")
    spark.sqlContext.setConf("spark.sql.parquet.mergeSchema", "false")
    spark.sqlContext.setConf("spark.sql.parquet.autoBroadcastJoinThreshold", "2147483000") // slightly below 2GB
    spark.sqlContext.setConf("spark.sql.broadcastTimeout", "10000") // seconds - enough time for big broadcast
    spark.sqlContext.setConf("spark.sql.parquet.compression.codec", "gzip")
    spark.sparkContext.hadoopConfiguration.set("parquet.summary.metadata.level", "NONE")
    spark
  }
}

