package com.ttd.mycellium.pipelines.bidrequest

import com.ttd.features.transformers.{Distinct, DropNA, Select, WithEmptyMap}
import com.ttd.mycellium.Vertex.{Advertiser, Campaign}
import org.apache.spark.ml.Pipeline
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.{ArrayType, FloatType, IntegerType, LongType, MapType, StringType}

class CampaignVertexPipe extends Pipeline {
  setStages(Array(
    DropNA(Array("CampaignId")),
    Select(Seq(
      unix_timestamp(date_trunc("day", col("LogEntryTime"))) as "ts",
      Campaign.vType as "v_type",
      Campaign.idColumn as "v_id",
      map(
        lit("CampaignId"), col("CampaignId")
      ) as "stringProps",
    )),
    WithEmptyMap("intProps", MapType(StringType, IntegerType)),
    WithEmptyMap("floatProps", MapType(StringType, FloatType)),
    WithEmptyMap("longProps", MapType(StringType, LongType)),
    WithEmptyMap("longArrayProps", MapType(StringType, ArrayType(LongType))),
    WithEmptyMap("floatArrayProps", MapType(StringType, ArrayType(FloatType))),
    WithEmptyMap("intArrayProps", MapType(StringType, ArrayType(IntegerType))),
    WithEmptyMap("stringArrayProps", MapType(StringType, ArrayType(StringType))),
    Distinct("v_id"),
  ))
}
