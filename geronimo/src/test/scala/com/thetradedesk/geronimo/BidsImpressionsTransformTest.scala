package com.thetradedesk.geronimo

import com.thetradedesk.geronimo.bidsimpression.transform.BidsImpressions
import com.thetradedesk.geronimo.shared.schemas.{AdvertiserRecord, BidFeedbackRecord, BidRequestRecord}
import com.thetradedesk.testutils.MockData
import com.thetradedesk.testutils.MockData.createImpressionsRow
import com.thetradedesk.testutils.TTDSparkTest
import com.thetradedesk.spark.TTDSparkContext.spark.implicits._

class BidsImpressionsTransformTest extends TTDSparkTest {

  test("BidsImpressions for Schema and Column Name Correctness") {

    val impOne = Seq(createImpressionsRow(BidRequestId = "1", SupplyVendor = "Google")).toDS().as[BidFeedbackRecord]
    val reqOne = Seq(MockData.request.copy(BidRequestId="1", AdvertiserId = Some("asdf"), SupplyVendor = Some("Google"))).toDS().as[BidRequestRecord]
    val advOne = Seq(MockData.advertiserRecordMock.copy(AdvertiserId = "asdf")).toDS().as[AdvertiserRecord]

    BidsImpressions.transform(reqOne, impOne, advOne)

}}
