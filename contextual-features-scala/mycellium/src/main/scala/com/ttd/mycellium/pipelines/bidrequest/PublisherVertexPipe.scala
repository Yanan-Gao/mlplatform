package com.ttd.mycellium.pipelines.bidrequest

import com.ttd.features.transformers.{Distinct, DropNA, Select, WithEmptyMap}
import com.ttd.mycellium.Vertex.Publisher
import org.apache.spark.ml.Pipeline
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.{ArrayType, FloatType, IntegerType, LongType, MapType, StringType}

class PublisherVertexPipe extends Pipeline {
  setStages(Array(
    DropNA(Array("SupplyVendorPublisherId")),
    Select(Seq(
      unix_timestamp(date_trunc("day", col("LogEntryTime"))) as "ts",
      Publisher.vType as "v_type",
      Publisher.idColumn as "v_id"
    )),
    WithEmptyMap("intProps", MapType(StringType, IntegerType)),
    WithEmptyMap("stringProps", MapType(StringType, StringType)),
    WithEmptyMap("floatProps", MapType(StringType, FloatType)),
    WithEmptyMap("longProps", MapType(StringType, LongType)),
    WithEmptyMap("longArrayProps", MapType(StringType, ArrayType(LongType))),
    WithEmptyMap("floatArrayProps", MapType(StringType, ArrayType(FloatType))),
    WithEmptyMap("intArrayProps", MapType(StringType, ArrayType(IntegerType))),
    WithEmptyMap("stringArrayProps", MapType(StringType, ArrayType(StringType))),
    Distinct("v_id"),
  ))
}
