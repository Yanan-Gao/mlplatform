package com.ttd.mycellium.pipelines.bidrequest

import com.ttd.features.transformers.{Distinct, DropNA, Select, WithColumn, WithEmptyMap}
import com.ttd.mycellium.Edge.AdvertiserOwnsCreative
import com.ttd.mycellium.Vertex.{Advertiser, Creative}
import org.apache.spark.ml.Pipeline
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.{ArrayType, FloatType, IntegerType, LongType, MapType, StringType}

class AdvertiserOwnsEdgesPipe extends Pipeline {
  setStages(Array(
    // Drop if can't associate with ids
    DropNA(Array("AdvertiserId", "CreativeId")),
    Select(Seq(
      unix_timestamp(date_trunc("day", col("LogEntryTime"))) as "ts",
      AdvertiserOwnsCreative.eType as "e_type",
      Advertiser.idColumn as "v_id1",
      Creative.idColumn as "v_id2",
      map(
        lit("AppStoreUrl"), col("AppStoreUrl"),
        lit("ReferrerUrl"), col("ReferrerUrl"),
        lit("RawUrl"), col("RawUrl"),
      ) as "stringProps",
    )),
    WithColumn("e_id", AdvertiserOwnsCreative.idColumn),
    WithEmptyMap("intProps", MapType(StringType, IntegerType)),
    WithEmptyMap("floatProps", MapType(StringType, FloatType)),
    WithEmptyMap("longProps", MapType(StringType, LongType)),
    WithEmptyMap("longArrayProps", MapType(StringType, ArrayType(LongType))),
    WithEmptyMap("floatArrayProps", MapType(StringType, ArrayType(FloatType))),
    WithEmptyMap("intArrayProps", MapType(StringType, ArrayType(IntegerType))),
    WithEmptyMap("stringArrayProps", MapType(StringType, ArrayType(StringType))),
    Distinct("e_id"),
  ))
}
