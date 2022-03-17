package com.ttd.mycellium.pipelines.conversiontracker

import com.ttd.features.transformers.{DropNA, Select, WithColumn, WithEmptyMap}
import com.ttd.mycellium.Edge.CampaignConvertsUserID
import com.ttd.mycellium.Vertex.{Campaign, UserID}
import org.apache.spark.ml.Pipeline
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.{ArrayType, FloatType, IntegerType, LongType, MapType, StringType}

class ConvertsEdgesPipe extends Pipeline {
  setStages(Array(
    // Drop if can't associate with any IPAddress
    DropNA(Array("CampaignId")),
    Select(Seq(
      col("ts"),
      CampaignConvertsUserID.eType as "e_type",
      Campaign.idColumn as "v_id1",
      UserID.idColumnForConversionTracker as "v_id2",
      map(
        lit("AdvertiserId"), col("AdvertiserId"),
        lit("ConversionType"), col("ConversionType")
      ) as "stringProps",
    )),
    WithColumn("e_id", CampaignConvertsUserID.idColumn),
    WithEmptyMap("intProps", MapType(StringType, IntegerType)),
    WithEmptyMap("floatProps", MapType(StringType, FloatType)),
    WithEmptyMap("longProps", MapType(StringType, LongType)),
    WithEmptyMap("longArrayProps", MapType(StringType, ArrayType(LongType))),
    WithEmptyMap("floatArrayProps", MapType(StringType, ArrayType(FloatType))),
    WithEmptyMap("intArrayProps", MapType(StringType, ArrayType(IntegerType))),
    WithEmptyMap("stringArrayProps", MapType(StringType, ArrayType(StringType))),
  ))
}
