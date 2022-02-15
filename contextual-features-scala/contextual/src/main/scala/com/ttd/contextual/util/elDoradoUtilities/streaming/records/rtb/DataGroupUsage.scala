package com.ttd.contextual.util.elDoradoUtilities.streaming.records.rtb

/**
 * @param id Id. The ID of the data group that was used.
 * @param rc Recency. In minutes. The recency of the user in the data group.
 * @param rbs RecencyBucketStart. In minutes. The lower bound of the recency bucket this recency falls into.
 * @param rbe RecencyBucketEnd. In minutes. The upper bound of the recency bucket this recency falls into.
 * @param deu DataElementUsages. The list of data elements in this data group that were used to match the user.
 * @param iadpic IncludeAllDataProvidersInCharges. Determines if most expensive element is chosen from each data providers, rather than the cheapest one.
 */
case class DataGroupUsage(id: Option[String], rc: Option[Int], rbs: Option[Int], rbe: Option[Int], deu: Option[Seq[ElementUsage]], iadpic: Boolean)
