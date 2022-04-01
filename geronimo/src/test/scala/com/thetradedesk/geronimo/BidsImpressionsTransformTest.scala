package com.thetradedesk.geronimo

import com.thetradedesk.geronimo.bidsimpression.transform.BidsImpressions
import com.thetradedesk.geronimo.shared.schemas.{BidRequestRecord, BidFeedbackRecord}
import com.thetradedesk.testutils.MockData
import com.thetradedesk.testutils.MockData.createImpressionsRow
import com.thetradedesk.testutils.TTDSparkTest
import com.thetradedesk.spark.TTDSparkContext.spark.implicits._

class BidsImpressionsTransformTest extends TTDSparkTest {

  test("BidsImpressions for Schema and Column Name Correctness") {

    val impOne = Seq(createImpressionsRow(BidRequestId = "1", SupplyVendor = "Google")).toDS().as[BidFeedbackRecord]
    val reqOne = Seq(MockData.request.copy(BidRequestId="1", SupplyVendor = Some("Google"))).toDS().as[BidRequestRecord]

    BidsImpressions.transform(reqOne, impOne)

}}
