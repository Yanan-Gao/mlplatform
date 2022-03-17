package com.ttd.mycellium.pipelines.bidrequest

import com.ttd.features.transformers.{Distinct, DropNA, Select, WithColumn, WithEmptyMap}
import com.ttd.mycellium.Edge.AdGroupUsesCreative
import com.ttd.mycellium.Vertex.{AdGroup, Creative}
import org.apache.spark.ml.Pipeline
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.{ArrayType, FloatType, IntegerType, LongType, MapType, StringType}

class AdGroupUsesEdgesPipe extends Pipeline {
  setStages(Array(
    // Drop if can't associate with any site
    DropNA(Array("AdGroupId", "CreativeId")),
    Select(Seq(
      unix_timestamp(date_trunc("day", col("LogEntryTime"))) as "ts",
      AdGroupUsesCreative.eType as "e_type",
      AdGroup.idColumn as "v_id1",
      Creative.idColumn as "v_id2",
      map(
        lit("AppStoreUrl"), col("AppStoreUrl"),
        lit("ReferrerUrl"), col("ReferrerUrl"),
        lit("RawUrl"), col("RawUrl"),
      ) as "stringProps",
    )),
    WithEmptyMap("intProps", MapType(StringType, IntegerType)),
    WithEmptyMap("floatProps", MapType(StringType, FloatType)),
    WithEmptyMap("longProps", MapType(StringType, LongType)),
    WithEmptyMap("longArrayProps", MapType(StringType, ArrayType(LongType))),
    WithEmptyMap("floatArrayProps", MapType(StringType, ArrayType(FloatType))),
    WithEmptyMap("intArrayProps", MapType(StringType, ArrayType(IntegerType))),
    WithEmptyMap("stringArrayProps", MapType(StringType, ArrayType(StringType))),
    WithColumn("e_id", AdGroupUsesCreative.idColumn),
    Distinct("e_id"),
  ))
}
