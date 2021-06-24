package com.thetradedesk.mlplatform.fsclient;
import com.thetradedesk.mlplatform.common.featurestore.Feature;
import com.thetradedesk.mlplatform.common.featurestore.FeatureLocation;
import com.thetradedesk.mlplatform.common.featurestore.FeatureType;
import org.apache.commons.lang.NotImplementedException;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.broadcast.Broadcast;
import org.apache.spark.sql.*;
import org.apache.spark.util.DoubleAccumulator;
import org.apache.spark.util.LongAccumulator;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.web.client.RestTemplateBuilder;
import org.springframework.http.ResponseEntity;
import org.springframework.web.client.RestTemplate;

import javax.xml.crypto.Data;
import java.io.IOException;


public class FeatureStoreClient
{
    private final FeatureStoreClientConfig Config;
    private final RestTemplate RestTemplate;

    public FeatureStoreClient(FeatureStoreClientConfig config)
    {
        this.Config = config;
        this.RestTemplate = new RestTemplateBuilder().rootUri(this.Config.getAPIPath()).build();
    }

    public Long getFeatureId(String featureName)
    {
        // Call the API and get a feature id from a feature name;
        // TODO: Add an API call specifically to query feature id from feature name - it'll be faster than listing all features
        ResponseEntity<Feature[]> response = this.RestTemplate.getForEntity("/features", Feature[].class);
        Feature[] responseBody = response.getBody();
        if(responseBody == null) return null;
        for(Feature f : responseBody)
        {
            if(f.getFeatureName().equals(featureName))
            {
                return f.getFeatureId();
            }
        }
        // If feature is not found - return null;
        return null;
    }

    // TODO: these interfaces are not set in stone yet - need to sync with the DB design and add/remove arguments
    // TODO - support multiple timestamps
    public Dataset<Row> ReadFeature(SparkSession spark, String featureName, Long version, Long timestamp) throws IOException
    {
        return this.ReadFeature(spark, this.getFeatureId(featureName), version, timestamp);
    }

    // TODO - support multiple timestamps
    public Dataset<Row> ReadFeature(SparkSession spark, Long featureId, Long version, Long timestamp) throws NotImplementedException, IOException
    {
        ResponseEntity<Feature> featureResponse = this.RestTemplate.getForEntity("/features/"+featureId, Feature.class);

        if(!featureResponse.hasBody())
        {
            // probably should have custom exceptions for not finding a feature
            throw new IOException("Feature not found!");
        }

        Feature feature = featureResponse.getBody();

        // TODO: create feature path based on location - timestamps and versions will get appended to feature path
        // TODO Handle versions and timestamps from API
        // TODO Handle AWS authentication if the FeatureLocation in S3 and AWS credentials are specified in the client config

        switch (feature.FeatureType)
        {
            case Parquet:
                // TODO: when we actually read features from multiple timestamps, we'll have multiple feature paths here to handle
                return spark.read().parquet(feature.FeaturePath);
            default:
                throw new NotImplementedException(String.format("ReadFeature for type `%s` not implemented", feature.FeatureType.name()));
        }
    }

}
