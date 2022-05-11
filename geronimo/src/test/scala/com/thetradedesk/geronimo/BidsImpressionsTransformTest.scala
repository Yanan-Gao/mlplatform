package com.thetradedesk.geronimo

import com.thetradedesk.geronimo.bidsimpression.schema.ContextualCategoryRecord
import com.thetradedesk.geronimo.bidsimpression.transform.BidsImpressions
import com.thetradedesk.geronimo.shared.schemas.{AdvertiserRecord, BidFeedbackRecord, BidRequestRecord}
import com.thetradedesk.testutils.MockData
import com.thetradedesk.testutils.MockData.createImpressionsRow
import com.thetradedesk.testutils.TTDSparkTest
import com.thetradedesk.spark.TTDSparkContext.spark.implicits._

class BidsImpressionsTransformTest extends TTDSparkTest {

  test("BidsImpressions for Schema and Column Name Correctness") {

    val impOne = Seq(createImpressionsRow(BidRequestId = "1", SupplyVendor = "Google")).toDS().as[BidFeedbackRecord]
    val reqOne = Seq(MockData.request.copy(BidRequestId = "1", AdvertiserId = Some("asdf"), SupplyVendor = Some("Google"))).toDS().as[BidRequestRecord]
    val advOne = Seq(MockData.advertiserRecordMock.copy(AdvertiserId = "asdf")).toDS().as[AdvertiserRecord]
    val contextual = Seq(MockData.contextual.copy(), MockData.contextual.copy(ContextualCategoryId = 267891234)).toDS().as[ContextualCategoryRecord]

    val (bidsImpressions, metrics) = BidsImpressions.transform(reqOne, impOne, advOne, contextual, 10, 10)
    assert(bidsImpressions.first.ContextualCategories.getOrElse(None) == Seq(123456789, 267891234))
  }
}
