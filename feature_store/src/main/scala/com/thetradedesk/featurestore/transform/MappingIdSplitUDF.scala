package com.thetradedesk.featurestore.transform

import org.apache.spark.sql.functions.udf

object MappingIdSplitUDF {

  //[slot * 65536, slot * 65536 + 65536)
  def apply(slot: Int) = {
    val start = slot * 65536
    val end = start + 65536
    udf(
      (mappingId: Seq[Int]) =>
        mappingId.filter(e => e >= start && e < end).map(e => (e - start).shortValue())
    )
  }
}
