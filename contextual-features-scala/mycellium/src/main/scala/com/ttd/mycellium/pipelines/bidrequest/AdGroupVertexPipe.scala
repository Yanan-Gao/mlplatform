package com.ttd.mycellium.pipelines.bidrequest

import com.ttd.features.transformers.{Distinct, DropNA, Select, WithEmptyMap}
import com.ttd.mycellium.Vertex.AdGroup
import org.apache.spark.ml.Pipeline
import org.apache.spark.sql.functions.{col, date_trunc, lit, map, typedLit, unix_timestamp}
import org.apache.spark.sql.types.{ArrayType, FloatType, IntegerType, LongType, MapType, StringType}

class AdGroupVertexPipe extends Pipeline {
  setStages(Array(
    DropNA(Array("AdGroupId")),
    Select(Seq(
      unix_timestamp(date_trunc("day", col("LogEntryTime"))) as "ts",
      AdGroup.vType as "v_type",
      AdGroup.idColumn as "v_id",
      map(
        lit("AdGroupId"), col("AdGroupId"),
        lit("AdvertiserId"), col("AdvertiserId"),
        lit("CampaignId"), col("CampaignId"),
        lit("PartnerId"), col("PartnerId"),
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
