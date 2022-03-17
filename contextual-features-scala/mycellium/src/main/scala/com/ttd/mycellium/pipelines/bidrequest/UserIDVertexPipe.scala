package com.ttd.mycellium.pipelines.bidrequest

import com.ttd.features.transformers.{Distinct, Select, WithEmptyMap}
import com.ttd.mycellium.Vertex.UserID
import org.apache.spark.ml.Pipeline
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.{ArrayType, FloatType, IntegerType, LongType, MapType, StringType}

class UserIDVertexPipe extends Pipeline {
  setStages(Array(
    Select(Seq(
      col("ts"),
      UserID.vType as "v_type",
      UserID.idColumn as "v_id",
      map(
        lit("Browser"), col("Browser.value"),
        lit("DeviceType"), col("DeviceType.value"),
        lit("DeviceModel"), col("DeviceModel"),
        lit("OperatingSystem"), col("OperatingSystem.value"),
        lit("OperatingSystemFamily"), col("OperatingSystemFamily.value"),
      ) as "intProps",
      map(
        lit("TDID"), col("TDID"),
        lit("UnifiedId2"), col("UnifiedId2"),
        lit("IdentityLinkId"), col("IdentityLinkId"),
        lit("DeviceAdvertisingId"), col("DeviceAdvertisingId"),
      ) as "stringProps",
    )),
    WithEmptyMap("floatProps", MapType(StringType, FloatType)),
    WithEmptyMap("longProps", MapType(StringType, LongType)),
    WithEmptyMap("longArrayProps", MapType(StringType, ArrayType(LongType))),
    WithEmptyMap("floatArrayProps", MapType(StringType, ArrayType(FloatType))),
    WithEmptyMap("intArrayProps", MapType(StringType, ArrayType(IntegerType))),
    WithEmptyMap("stringArrayProps", MapType(StringType, ArrayType(StringType))),
    Distinct("v_id"),
  ))
}
