package com.thetradedesk.featurestore.dataset

import com.thetradedesk.featurestore.constants.FeatureConstants
import com.thetradedesk.featurestore.datasets.ProcessedDataset
import com.thetradedesk.featurestore.testutils.TTDSparkTest
import com.thetradedesk.featurestore.ttdEnv
import org.apache.spark.sql.{Encoder, Encoders}
import org.scalatest.matchers.should.Matchers

import java.time.LocalDate
import java.time.format.DateTimeFormatter
import scala.reflect.runtime.universe._

class DataSetTest extends TTDSparkTest with Matchers {

  test("ProcessedDataset should read and write correctly") {
    val date = LocalDate.of(2024, 5, 24)

    val dateStr = date.format(DateTimeFormatter.ofPattern("yyyyMMdd"))

    var expect = s"${
      FeatureConstants
        .ML_PLATFORM_S3_PATH
    }/features/feature_store/${ttdEnv}/processed/somerecord/v=1/date=$dateStr"

    var writePath = TestProcessedDataset().datePartitionedPath(partition = Some(date))
    writePath shouldEqual (expect)

    writePath = TestProcessedDataset(15).datePartitionedPath(partition = Some(date))
    expect = s"${
      FeatureConstants
        .ML_PLATFORM_S3_PATH
    }/features/feature_store/${ttdEnv}/processed/somerecord/v=1/lookback=15d/date=$dateStr"
    writePath shouldEqual (expect)
    // todo add write and read test case
  }
}

case class SomeRecord(SomeId: String)

case class TestProcessedDataset(lb: Int = 0) extends ProcessedDataset[SomeRecord] {
  override val lookback = lb
  override val defaultNumPartitions: Int = 1
  override val datasetName: String = "somerecord"

  val enc: Encoder[SomeRecord] = Encoders.product[SomeRecord]
  val tt: TypeTag[SomeRecord] = typeTag[SomeRecord]
}
