package com.ttd.mycellium.pipelines.bidrequest

import com.ttd.features.transformers.{Distinct, DropNA, Select, WithColumn, WithEmptyMap}
import com.ttd.mycellium.Edge.{PublisherHasSite, UserIDConnectedFromIPAddress}
import com.ttd.mycellium.Vertex.{IPAddress, Publisher, Site, UserID}
import org.apache.spark.ml.Pipeline
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.{ArrayType, FloatType, IntegerType, LongType, MapType, StringType}

class PublisherHasSiteEdgesPipe extends Pipeline {
  setStages(Array(
    // Drop if can't associate with any site
    DropNA(Array("Site", "SupplyVendorPublisherId")),
    Select(Seq(
      unix_timestamp(date_trunc("day", col("LogEntryTime"))) as "ts",
      PublisherHasSite.eType as "e_type",
      Publisher.idColumn as "v_id1",
      Site.idColumn as "v_id2",
    )),
    WithColumn("e_id", PublisherHasSite.idColumn),
    WithEmptyMap("intProps", MapType(StringType, IntegerType)),
    WithEmptyMap("stringProps", MapType(StringType, StringType)),
    WithEmptyMap("floatProps", MapType(StringType, FloatType)),
    WithEmptyMap("longProps", MapType(StringType, LongType)),
    WithEmptyMap("longArrayProps", MapType(StringType, ArrayType(LongType))),
    WithEmptyMap("floatArrayProps", MapType(StringType, ArrayType(FloatType))),
    WithEmptyMap("intArrayProps", MapType(StringType, ArrayType(IntegerType))),
    WithEmptyMap("stringArrayProps", MapType(StringType, ArrayType(StringType))),
    Distinct("e_id"),
  ))
}
