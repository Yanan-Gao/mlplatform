package com.ttd.mycellium.pipelines.bidrequest

import com.ttd.features.transformers.{Distinct, DropNA, Select, WithColumn, WithEmptyMap}
import com.ttd.mycellium.Edge.{SupplyVendorHasPublisher, UserIDConnectedFromIPAddress}
import com.ttd.mycellium.Vertex.{Publisher, Site, SupplyVendor}
import org.apache.spark.ml.Pipeline
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.{ArrayType, FloatType, IntegerType, LongType, MapType, StringType}

class SupplyVendorHasPublisherEdgesPipe extends Pipeline {
  setStages(Array(
    // Drop if can't associate with any site
    DropNA(Array("SupplyVendor", "SupplyVendorPublisherId")),
    Select(Seq(
      unix_timestamp(date_trunc("day", col("LogEntryTime"))) as "ts",
      SupplyVendorHasPublisher.eType as "e_type",
      SupplyVendor.idColumn as "v_id1",
      Publisher.idColumn as "v_id2"
    )),
    WithEmptyMap("intProps", MapType(StringType, IntegerType)),
    WithEmptyMap("stringProps", MapType(StringType, StringType)),
    WithEmptyMap("floatProps", MapType(StringType, FloatType)),
    WithColumn("e_id", SupplyVendorHasPublisher.idColumn),
    WithEmptyMap("longProps", MapType(StringType, LongType)),
    WithEmptyMap("longArrayProps", MapType(StringType, ArrayType(LongType))),
    WithEmptyMap("floatArrayProps", MapType(StringType, ArrayType(FloatType))),
    WithEmptyMap("intArrayProps", MapType(StringType, ArrayType(IntegerType))),
    WithEmptyMap("stringArrayProps", MapType(StringType, ArrayType(StringType))),
    Distinct("e_id"),
  ))
}
