package com.ttd.ml.util

import com.ttd.ml.spark.TTDSparkContext
import org.scalatest.BeforeAndAfterAll
import org.apache.spark.sql.SparkSession

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

