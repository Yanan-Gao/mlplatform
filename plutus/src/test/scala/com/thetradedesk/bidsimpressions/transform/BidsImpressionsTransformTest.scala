package com.thetradedesk.bidsimpressions.transform

import com.thetradedesk.TestUtils.TTDSparkTest
import com.thetradedesk.bidsimpression.transform.BidsImpressions
import com.thetradedesk.plutus.data.MockData
import com.thetradedesk.plutus.data.MockData.createImpressionsRow
import com.thetradedesk.plutus.data.schema.{BidRequestRecord, Impressions}
import org.apache.spark.sql.Dataset
import com.thetradedesk.spark.TTDSparkContext.spark.sparkContext
import com.thetradedesk.spark.TTDSparkContext.spark.implicits._

class BidsImpressionsTransformTest extends TTDSparkTest {

  test("BidsImpressions for Schema and Column Name Correctness") {

    val impOne = Seq(createImpressionsRow(BidRequestId = "1", SupplyVendor = "Google")).toDS().as[Impressions]
    val reqOne = Seq(MockData.request.copy(BidRequestId="1", SupplyVendor = Some("Google"))).toDS().as[BidRequestRecord]

    BidsImpressions.transform(java.time.LocalDate.parse("2021-12-12"), "s3://fake/bucket/", "", "", reqOne, impOne, Seq(1, 2) )

}}
