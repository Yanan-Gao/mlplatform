package com.ttd.mycellium.pipelines.bidrequest

import com.ttd.features.transformers.{DropNA, Select, WithColumn, WithEmptyMap}
import com.ttd.mycellium.Edge.GeoContainsLatLong
import com.ttd.mycellium.Vertex.{GeoLocation, LatLong}
import org.apache.spark.ml.Pipeline
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.{ArrayType, FloatType, IntegerType, LongType, MapType, StringType}

class GeoContainsEdgesPipe extends Pipeline {
  setStages(Array(
    // Drop if can't associate with any IPAddress
    DropNA(Array("Latitude", "Longitude", "Zip"), "any"),
    Select(Seq(
      col("ts"),
      GeoContainsLatLong.eType as "e_type",
      GeoLocation.idColumn as "v_id1",
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
    WithColumn("e_id", GeoContainsLatLong.idColumn),
  ))
}
