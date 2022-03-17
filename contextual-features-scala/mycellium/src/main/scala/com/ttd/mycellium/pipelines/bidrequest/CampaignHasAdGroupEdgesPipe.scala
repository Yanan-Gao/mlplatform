package com.ttd.mycellium.pipelines.bidrequest

import com.ttd.features.transformers.{Distinct, DropNA, Select, WithColumn, WithEmptyMap}
import com.ttd.mycellium.Edge.CampaignHasAdGroup
import com.ttd.mycellium.Vertex.{AdGroup, Campaign}
import org.apache.spark.ml.Pipeline
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.{ArrayType, FloatType, IntegerType, LongType, MapType, StringType}

class CampaignHasAdGroupEdgesPipe extends Pipeline {
  setStages(Array(
    // Drop if can't associate with ids
    DropNA(Array("AdGroupId", "CampaignId")),
    Select(Seq(
      unix_timestamp(date_trunc("day", col("LogEntryTime"))) as "ts",
      CampaignHasAdGroup.eType as "e_type",
      Campaign.idColumn as "v_id1",
      AdGroup.idColumn as "v_id2",
    )),
    WithEmptyMap("intProps", MapType(StringType, IntegerType)),
    WithEmptyMap("stringProps", MapType(StringType, StringType)),
    WithEmptyMap("floatProps", MapType(StringType, FloatType)),
    WithEmptyMap("longProps", MapType(StringType, LongType)),
    WithEmptyMap("longArrayProps", MapType(StringType, ArrayType(LongType))),
    WithEmptyMap("floatArrayProps", MapType(StringType, ArrayType(FloatType))),
    WithEmptyMap("intArrayProps", MapType(StringType, ArrayType(IntegerType))),
    WithEmptyMap("stringArrayProps", MapType(StringType, ArrayType(StringType))),
    WithColumn("e_id", CampaignHasAdGroup.idColumn),
    Distinct("e_id"),
  ))
}
