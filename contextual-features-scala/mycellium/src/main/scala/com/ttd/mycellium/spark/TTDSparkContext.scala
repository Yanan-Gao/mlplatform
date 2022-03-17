package com.ttd.mycellium.spark

import org.apache.spark.sql.SparkSession

object TTDSparkContext {
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
          .config("spark.master", "local")
          .config("spark.sql.shuffle.partitions", 1)
          .getOrCreate()
      else
        SparkSession
          .builder()
          .config("fs.s3a.connection.maximum", 5000)
          .getOrCreate()

//    spark.sparkContext.hadoopConfiguration.set("mapreduce.fileoutputcommitter.algorithm.version", "2")
    spark.sqlContext.setConf("spark.sql.parquet.filterPushdown", "true")
    spark.sqlContext.setConf("spark.sql.parquet.mergeSchema", "false")
    spark.sqlContext.setConf("spark.sql.parquet.autoBroadcastJoinThreshold", "2147483000") // slightly below 2GB
    spark.sqlContext.setConf("spark.sql.broadcastTimeout", "10000") // seconds - enough time for big broadcast

//    Compression Ratio : GZIP compression uses more CPU resources than Snappy or LZO, but provides a higher compression ratio.
//    General Usage : GZip is often a good choice for cold data, which is accessed infrequently. Snappy or LZO are a better choice for hot data, which is accessed frequently.
//    spark.sqlContext.setConf("spark.sql.parquet.compression.codec", "gzip")
    spark.sparkContext.hadoopConfiguration.set("parquet.summary.metadata.level", "NONE")

    spark
  }
}
