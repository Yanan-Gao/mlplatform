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
        clientConfig.setAPIPath("http://localhost:8083");
        FeatureStoreClient client = new FeatureStoreClient(clientConfig);

        Dataset<Row> ds = client.ReadFeature(spark, "TestLocalFeature", 1L, 1L);
        //Dataset<Row> ds = client.ReadFeature(spark, "UserProfiles", 1L, 1L);

        ds.printSchema();


    }

    //@Test
    public void APIGraphTest() throws IOException
    {

    FeatureStoreClientConfig clientConfig = new FeatureStoreClientConfig();
    //clientConfig.setAPIPath("https://mlplatform.dev.gen.adsrvr.org");
    clientConfig.setAPIPath("http://localhost:8083");
    FeatureStoreClient client = new FeatureStoreClient(clientConfig);

    long callCounter = 0;
    while(true)
    {
        client.getFeatureId("TestLocalFeature");
        callCounter++;
        if(callCounter % 100 == 0)
        {
            System.err.println(String.format("Called getFeatureId %d times", callCounter));
        }
    }



}

}
