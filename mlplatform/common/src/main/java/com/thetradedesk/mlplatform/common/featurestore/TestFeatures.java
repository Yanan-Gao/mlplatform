package com.thetradedesk.mlplatform.common.featurestore;

import java.util.HashMap;
import java.util.Map;

public class TestFeatures
{
    public static final Map<Long,Feature> Features = new HashMap<>();
    static {
        Feature f1 = new Feature();
        f1.setFeatureId(1L);
        f1.setFeatureName("UserProfiles");
        f1.setFeatureLocation(FeatureLocation.AmazonS3);
        f1.setFeatureType(FeatureType.Parquet);
        f1.setFeaturePath("s3://thetradedesk-useast-hadoop/unified-data-models/prod/user-profiles/avails");
        // TODO: we need to support different partition formats for different external features - this is just an example
        f1.setPartitionFormat("date={year}-{month}-{day}");

        Feature f2 = new Feature();
        f2.setFeatureId(2L);
        f2.setFeatureName("AvailsSiteSummaries");
        f2.setFeatureLocation(FeatureLocation.AmazonS3);
        f2.setFeatureType(FeatureType.TSV);
        f2.setFeaturePath("s3://thetradedesk-useast-hadoop/unified-data-models/prod/site-summaries/");
        // TODO: we need to support different partition formats for different external features - this is just an example
        f2.setPartitionFormat("{year}/{month}/{day}");
        // TODO: We also need to be able to specify a schema for formats that don't contain one
        // e.g. tsv without headers

        Feature f3 = new Feature();
        f3.setFeatureId(2L);
        f3.setFeatureName("TestLocalFeature");
        f3.setFeatureLocation(FeatureLocation.Local);
        f3.setFeatureType(FeatureType.Parquet);
        f3.setFeaturePath("c:\\temp\\alltypes_plain.parquet");

        Features.put(f1.getFeatureId(),f1);
        Features.put(f2.getFeatureId(),f2);
        Features.put(f3.getFeatureId(),f3);
    }
}
