package com.thetradedesk.mlplatform.fsclient;
import com.thetradedesk.mlplatform.common.featurestore.Feature;
import com.thetradedesk.mlplatform.common.featurestore.FeatureLocation;
import com.thetradedesk.mlplatform.common.featurestore.FeatureType;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.broadcast.Broadcast;
import org.apache.spark.sql.*;
import org.apache.spark.util.DoubleAccumulator;
import org.apache.spark.util.LongAccumulator;

import javax.xml.crypto.Data;
import java.io.IOException;


public class FeatureStoreClient
{
    private final FeatureStoreClientConfig Config;

    public FeatureStoreClient(FeatureStoreClientConfig config)
    {
        this.Config = config;
    }

    public Long getFeatureId(String featureName)
    {
        // TODO: Call the API and get a feature id from a feature name;
        // If feature is not found - return null;
        return 1L;
    }

    //TODO: these interfaces are not set in stone yet - need to discuss with the API team and add/remove

    public Dataset<Row> ReadFeature(SparkSession spark, String featureName, Long version, Long timestamp) throws IOException
    {
        return this.ReadFeature(spark, this.getFeatureId(featureName), version, timestamp);
    }

    public Dataset<Row> ReadFeature(SparkSession spark, Long featureId, Long version, Long timestamp) throws IOException
    {
        // TODO: Get feature details
        Feature feature = new Feature();
        feature.FeatureId = featureId;
        feature.FeatureLocation = FeatureLocation.Local;
        feature.FeatureType = FeatureType.Parquet;
        feature.FeaturePath = "c:\\temp\\alltypes_plain.parquet";

        // TODO: create feature path based on location - timestamps and versions will get appended to feature path

        switch (feature.FeatureType)
        {
            case Parquet:
                return spark.read().parquet(feature.FeaturePath);
            default:
                return null;
        }

    }

}
