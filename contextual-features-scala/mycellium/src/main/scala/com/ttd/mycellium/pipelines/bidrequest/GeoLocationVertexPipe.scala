package com.ttd.mycellium.pipelines.bidrequest

import com.ttd.features.transformers.{Distinct, DropNA, Select, WithColumn, WithEmptyMap}
import com.ttd.mycellium.Edge.GeoContainsLatLong
import com.ttd.mycellium.Vertex.{GeoLocation, LatLong}
import org.apache.spark.ml.Pipeline
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.{ArrayType, FloatType, IntegerType, LongType, MapType, StringType}

class GeoLocationVertexPipe extends Pipeline {
  setStages(Array(
    // Drop if can't associate with any IPAddress
    DropNA(Array("Latitude", "Longitude", "Zip"), "any"),
    Select(Seq(
      unix_timestamp(date_trunc("day", col("LogEntryTime"))) as "ts",
      GeoLocation.vType as "v_type",
      GeoLocation.idColumn as "v_id",
      map(
        lit("Country"), col("Country"),
        lit("Region"), col("Region"),
        lit("City"), col("City"),
        lit("Zip"), col("Zip"),
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
