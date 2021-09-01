package com.thetradedesk.mlplatform.common.featurestore;

import java.util.HashMap;
import java.util.Map;
import java.util.List;
import java.util.ArrayList;
import java.util.Date;

// TODO: we need to support different partition formats for different external features - this is just an example
//f1.setPartitionFormat("date={year}-{month}-{day}");
// TODO: We also need to be able to specify a schema for formats that don't contain one
// e.g. tsv without headers


public class TestFeatures
{
    public static final Map<Long,Feature> Features = new HashMap<>();
    static {
        Feature f1 = new Feature();
        f1.setFeatureId(1L);
        f1.setFeatureName("PlutusProfilesModelTestInput");
        f1.setFeatureDescription("Model Input Test Data");

        Feature f2 = new Feature();
        f2.setFeatureId(2L);
        f2.setFeatureName("PlutusProfilesModelTrainInput");
        f2.setFeatureDescription("Model Input Train Data");

        Feature f3 = new Feature();
        f3.setFeatureId(2L);
        f3.setFeatureName("WebEmbeddingsForContextual");
        f3.setFeatureDescription("blah blah");

        Features.put(f1.getFeatureId(),f1);
        Features.put(f2.getFeatureId(),f2);
        Features.put(f3.getFeatureId(),f3);

    }

    public static final ArrayList<FeatureVersion> FeatureVersions = new ArrayList<FeatureVersion>();
    static {
        FeatureVersion fv1_1 = new FeatureVersion();
        fv1_1.setFeatureId(1L);
        fv1_1.setFeatureVersion(1L);
        fv1_1.setCodePath("s3://thetradedesk-mlplatform-us-east-1/features/libs/plutus/prod/plutus.jar");
        fv1_1.setDataPath("s3://thetradedesk-mlplatform-us-east-1/features/data/plutus/v=1/prod/modelinput/google/year=2021/month=5/day=25/lookback=7/test/");
        fv1_1.setDocumentationPath("s3://somepath");
        fv1_1.setSchemaString("");

        FeatureVersion fv1_2 = new FeatureVersion();
        fv1_2.setFeatureId(1L);
        fv1_2.setFeatureVersion(2L);
        fv1_2.setCodePath("s3://thetradedesk-mlplatform-us-east-1/features/libs/plutus/snapshot/plutus.jar");
        fv1_2.setDataPath("s3://thetradedesk-mlplatform-us-east-1/features/data/plutus/v=2/prod/modelinput/google/year=2021/month=5/day=25/lookback=7/test/");
        fv1_2.setDocumentationPath("somePath");
        fv1_2.setSchemaString("");

        FeatureVersion fv2 = new FeatureVersion();
        fv2.setFeatureId(2L);
        fv2.setFeatureVersion(1L);
        fv2.setCodePath("s3://thetradedesk-mlplatform-us-east-1/features/libs/contextual-features-scala/snapshot/contextual-features-scala.jar");
        fv2.setDataPath("s3://thetradedesk-mlplatform-us-east-1/features/data/plutus/v=1/prod/modelinput/google/year=2021/month=5/day=25/lookback=7/train/");
        fv2.setDocumentationPath("somepath");
        fv2.setSchemaString("");

        FeatureVersion fv3 = new FeatureVersion();
        fv3.setFeatureId(3L);
        fv3.setFeatureVersion(1L);
        fv3.setCodePath("s3://thetradedesk-mlplatform-us-east-1/features/libs/contextual-features-scala/snapshot/contextual-features-scala.jar");
        fv3.setDataPath("s3://thetradedesk-mlplatform-us-east-1/feature_store/features/data/contextual/web/embeddings");
        fv3.setDocumentationPath("somepath");
        fv3.setSchemaString("");

        FeatureVersions.add(fv1_1);
        FeatureVersions.add(fv1_2);
        FeatureVersions.add(fv2);
        FeatureVersions.add(fv3);
    }
}
