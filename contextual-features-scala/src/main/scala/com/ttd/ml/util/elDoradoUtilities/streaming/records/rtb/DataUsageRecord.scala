package com.ttd.ml.util.elDoradoUtilities.streaming.records.rtb

/**
 * @param tc TotalCost. Describe the sum of usage costs of the recorded usage as the total cost, if recorded usage has been set.
 * @param aid Audience ID. The audience to which the data groups belong. Populated by the log extractor.
 * @param dgu DataGroupUsages. Describes data group targeted data matching user data for this impression.
 * @param rc Recency. The recency in minutes used to apply a recency adjustment in bidding.
 * @param sbab SelectedBidAdjustmentBrand. We charge on a single brand for data element bid adjustments from all the brands that were used for data element bid agjustments.
 * @param rbs RecencyBucketStart. In minutes. The lower bound of the recency bucket this recency falls into.
 * @param rbe RecencyBucketEnd. In minutes. The upper bound of the recency bucket this recency falls into.
 */
case class DataUsageRecord(tc: Option[BigDecimal] = None, aid: Option[String] = None, dgu: Seq[DataGroupUsage], rc: Option[Int] = None, sbab: Option[String] = None, rbs: Option[Int] = None, rbe: Option[Int] = None)
