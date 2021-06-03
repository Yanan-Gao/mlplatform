package com.thetradedesk.mlplatform.fsclient;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.junit.Test;

import java.io.IOException;


public class TestFSClient
{
    // If you need to test the feature store client locally, you can uncomment here
    // Do not commit a feature store client test as a UT unless you've set it up to read from local resources only
    //@Test
    public void RunTest() throws IOException
    {
        SparkSession.Builder sessionBuilder = SparkSession.builder()
                        .appName("TestFSClient")
                        .master("local");

        SparkSession spark = sessionBuilder.getOrCreate();

        FeatureStoreClientConfig clientConfig = new FeatureStoreClientConfig();
        FeatureStoreClient client = new FeatureStoreClient(clientConfig);

        Dataset<Row> ds = client.ReadFeature(spark, 1L, 1L, 1L);

        ds.printSchema();

    }

}
