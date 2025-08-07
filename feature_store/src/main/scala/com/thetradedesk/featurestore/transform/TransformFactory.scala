package com.thetradedesk.featurestore.transform

import com.thetradedesk.featurestore.datasets.DatasetReader
import com.thetradedesk.featurestore.utils.StringUtils.getDateFromString
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions.{col, explode}

trait Transformer {
    def transform(df: DataFrame): DataFrame
}

object TransformFactory {
    def getTransformers(transform: Seq[String], context: Map[String, String]): Seq[Transformer] = {
        transform.map {
            case "TDID_To_SeedId" => new SeedIdTransformer(context)
        }
    }
}

// this transformer take
class SeedIdTransformer(val context: Map[String, String]) extends Transformer {
    def transform(df: DataFrame): DataFrame = {
        val seedSourcePath = context("aggSeedDataSource")
        val startDate = getDateFromString(context("dateStr"))
        val aggSeeds = DatasetReader.readLatestDataset(seedSourcePath, startDate)
        df.withColumnRenamed("FeatureKey", "TDID")
          .join(aggSeeds.select("TDID", "SeedIds"), "TDID")
          .withColumn("FeatureKey", explode(col("SeedIds")))
          .drop("SeedIds")
    }
}
