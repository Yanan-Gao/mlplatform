package com.ttd.mycellium.pipelines.bidfeedback

import com.ttd.features.transformers.{DropNA, Select, WithColumn, WithEmptyMap}
import com.ttd.mycellium.Edge.AdGroupServesUserID
import com.ttd.mycellium.Vertex.{AdGroup, UserID}
import org.apache.spark.ml.Pipeline
import org.apache.spark.sql.functions.{col, lit, map, typedLit}
import org.apache.spark.sql.types.{ArrayType, FloatType, IntegerType, LongType, MapType, StringType}

class ServesEdgesPipe extends Pipeline {
  setStages(Array(
    // Drop if can't associate with any AdGroupId
    DropNA(Array("AdGroupId")),
    Select(Seq(
      col("ts"),
      AdGroupServesUserID.eType as "e_type",
      AdGroup.idColumn as "v_id1",
      UserID.idColumn as "v_id2",
      map(
        lit("ChannelId"), col("ChannelId"),
        lit("CreativeId"), col("CreativeId"),
        lit("CampaignId"), col("CampaignId"),
        lit("AdvertiserId"), col("AdvertiserId"),
      ) as "stringProps",
    )),
    WithEmptyMap("intProps", MapType(StringType, IntegerType)),
    WithEmptyMap("floatProps", MapType(StringType, FloatType)),
    WithEmptyMap("longProps", MapType(StringType, LongType)),
    WithEmptyMap("longArrayProps", MapType(StringType, ArrayType(LongType))),
    WithEmptyMap("floatArrayProps", MapType(StringType, ArrayType(FloatType))),
    WithEmptyMap("intArrayProps", MapType(StringType, ArrayType(IntegerType))),
    WithEmptyMap("stringArrayProps", MapType(StringType, ArrayType(StringType))),
    WithColumn("e_id", AdGroupServesUserID.idColumn)
  ))
}
