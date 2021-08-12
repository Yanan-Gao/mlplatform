package com.ttd.ml.util.elDoradoUtilities.streaming.records.rtb

/**
 * @param id Id. Id for the targeting data used by the impression.
 * @param tpb
 * @param c  Cost. Indicates the cost for this usage. This won't be populated until costs have been looked up for the impression downstream.
 * @param tpid ThirdPartyDataProviderElementId. If set, provides the identifier for this data used by the third party data provider
 * @param used Used. If true, indicates that the element is recorded as 'used' to account for the total cost of this record.
 * @param rrs  RateRevShare. If set, indicates the 'revshare' percentage to use as the cost of this data.  Exactly one of 'rrs' and 'rcpm' should be set on an instance of ElementUsage.
 * @param rcpm RateCPM. If set, indicates the cpm rate to use to cost this data.   Exactly one of 'rrs' and 'rcpm' should be set on an instance of ElementUsage.
 * @param vcp Volume Control Priority
 * @param drcid DataRateCardId of the rate used for this element
 */
case class ElementUsage(id: Long, tpb: Option[String], c: Option[BigDecimal], tpid: Option[String], used: Option[Boolean], rrs: Option[BigDecimal], rcpm: Option[BigDecimal], vcp: Option[Int], drcid: Option[Long])
