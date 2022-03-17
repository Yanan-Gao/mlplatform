package com.ttd.mycellium.pipelines.bidrequest

import com.ttd.features.transformers.{DropNA, Select, WithColumn, WithEmptyMap}
import com.ttd.mycellium.Edge.UserIDTrackedAtLatLong
import com.ttd.mycellium.Vertex.{LatLong, UserID}
import org.apache.spark.ml.Pipeline
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.{ArrayType, FloatType, IntegerType, LongType, MapType, StringType}

class TrackedAtEdgesPipe extends Pipeline {
  setStages(Array(
    // Drop if can't associate with any location
    DropNA(Array("Latitude", "Longitude")),
    Select(Seq(
      col("ts"),
      UserIDTrackedAtLatLong.eType as "e_type",
      UserID.idColumn as "v_id1",
      LatLong.idColumn as "v_id2"
    )),
    WithEmptyMap("intProps", MapType(StringType, IntegerType)),
    WithEmptyMap("stringProps", MapType(StringType, StringType)),
    WithEmptyMap("floatProps", MapType(StringType, FloatType)),
    WithEmptyMap("longProps", MapType(StringType, LongType)),
    WithEmptyMap("longArrayProps", MapType(StringType, ArrayType(LongType))),
    WithEmptyMap("floatArrayProps", MapType(StringType, ArrayType(FloatType))),
    WithEmptyMap("intArrayProps", MapType(StringType, ArrayType(IntegerType))),
    WithEmptyMap("stringArrayProps", MapType(StringType, ArrayType(StringType))),
    WithColumn("e_id", UserIDTrackedAtLatLong.idColumn)
  ))
}
