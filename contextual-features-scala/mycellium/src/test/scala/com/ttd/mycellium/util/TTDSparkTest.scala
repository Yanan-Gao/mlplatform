package com.ttd.mycellium.util

import org.apache.spark.sql.SparkSession
import org.scalatest.BeforeAndAfterAll

trait TTDSparkTest extends UnitSpec with BeforeAndAfterAll {
  TTDSparkContext.setTestMode() // run this on class creation and before TTDSparkContext.spark is ever referenced
  val spark: SparkSession = TTDSparkContext.spark // access the SparkSession immediately, before anything else has a chance to create one

  // unpersist data after each test
  override def afterAll(): Unit = {
    println(s"${this.suiteId} contains ${spark.sparkContext.getPersistentRDDs.size} unpersisted RDDs")
    for ( (id,rdd) <- spark.sparkContext.getPersistentRDDs ) {
      rdd.unpersist()
    }
  }
}

object TTDSparkContext {
  private var isTestMode = false;

  def setTestMode(): Unit = {
    isTestMode = true
  }

  lazy val spark: SparkSession = getSpark

  private def getSpark: SparkSession = {
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

    spark
  }
}