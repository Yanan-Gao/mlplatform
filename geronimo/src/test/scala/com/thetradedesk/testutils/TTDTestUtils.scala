package com.thetradedesk.testutils

import com.thetradedesk.spark.TTDSparkContext
import org.scalatest._
import org.scalatest.{BeforeAndAfterAll, BeforeAndAfterEach}
import com.thetradedesk.spark.TTDSparkContext.spark.implicits._
import com.thetradedesk.spark.util.{RichConfig, TTDConfig}
import org.apache.spark.sql.Dataset
import org.scalatest.funsuite.AnyFunSuite


//TODO Note this is a direct copy of the TTDTestUtils from el-dorado and should be replaced by a dependency on el-dorado core
// once it is available
trait TTDSparkTest extends AnyFunSuite with BeforeAndAfterAll with BeforeAndAfterEach {
  TTDSparkContext.setTestMode() // run this on class creation and before TTDSparkContext.spark is ever referenced
  val spark = TTDSparkContext.spark // access the SparkSession immediately, before anything else has a chance to create one

  override def beforeEach(): Unit = {
    TTDConfig.config = RichConfig.default // clear config so any set variables are not carried over
  }

  // unpersist data after each test
  override def afterAll(): Unit = {
    println(s"${this.suiteId} contains ${spark.sparkContext.getPersistentRDDs.size} unpersisted RDDs")
    for ((id, rdd) <- spark.sparkContext.getPersistentRDDs) {
      rdd.unpersist()
    }
  }

  def ds[T <: Product](record: T)(implicit m: Manifest[T]): Dataset[T] = {
    spark.createDataset(Seq(record))
  }
}


