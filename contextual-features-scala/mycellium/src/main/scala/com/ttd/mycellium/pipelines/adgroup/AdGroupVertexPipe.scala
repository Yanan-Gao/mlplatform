package com.ttd.mycellium.pipelines.adgroup

import com.ttd.features.transformers.{Distinct, Select, WithEmptyMap}
import com.ttd.mycellium.Vertex.AdGroup
import org.apache.spark.ml.Pipeline
import org.apache.spark.sql.functions.{col, lit, map, typedLit, unix_timestamp}
import org.apache.spark.sql.types.{ArrayType, FloatType, IntegerType, LongType, MapType, StringType}

class AdGroupVertexPipe extends Pipeline {
  setStages(Array(
    Select(Seq(
      col("ts"),
      AdGroup.vType as "v_type",
      AdGroup.idColumn as "v_id",
      map(
        lit("CreatedAt"), unix_timestamp(col("CreatedAt")),
        lit("LastUpdatedAt"), unix_timestamp(col("LastUpdatedAt"))
      ) as "intProps",
      map(
        lit("AdGroupId"), col("AdGroupId"),
        lit("AdvertiserId"), col("AdvertiserId"),
        lit("AdGroupName"), col("AdGroupName"),
        lit("AdGroupDescription"), col("AdGroupDescription"),
        lit("AdGroupTypeId"), col("AdGroupTypeId"),
        lit("CampaignId"), col("CampaignId"),
        lit("PartnerId"), col("PartnerId"),
      ) as "stringProps",
    )),
    Distinct("v_id"),
    WithEmptyMap("floatProps", MapType(StringType, FloatType)),
    WithEmptyMap("longProps", MapType(StringType, LongType)),
    WithEmptyMap("longArrayProps", MapType(StringType, ArrayType(LongType))),
    WithEmptyMap("floatArrayProps", MapType(StringType, ArrayType(FloatType))),
    WithEmptyMap("intArrayProps", MapType(StringType, ArrayType(IntegerType))),
    WithEmptyMap("stringArrayProps", MapType(StringType, ArrayType(StringType))),
  ))
}
