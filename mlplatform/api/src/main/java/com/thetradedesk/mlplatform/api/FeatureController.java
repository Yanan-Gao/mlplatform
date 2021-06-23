package com.thetradedesk.mlplatform.api;

import com.thetradedesk.mlplatform.common.featurestore.Feature;
import com.thetradedesk.mlplatform.common.featurestore.FeatureLocation;
import com.thetradedesk.mlplatform.common.featurestore.FeatureType;
import org.springframework.web.bind.annotation.*;

import java.util.*;

@RestController
public class FeatureController
{
    // TODO: Remove this and run everything from the DB - this is here for testing API calls prior to DB setup
    private static final Map<Long,Feature> TestFeatures = new HashMap<>();
    static {
        Feature f1 = new Feature();
        f1.setFeatureId(1L);
        f1.setFeatureName("User Profiles");
        f1.setFeatureLocation(FeatureLocation.AmazonS3);
        f1.setFeatureType(FeatureType.Parquet);
        f1.setFeaturePath("s3://thetradedesk-useast-hadoop/unified-data-models/prod/user-profiles/avails");
        // TODO: we need to support different partition formats for different external features - this is just an example
        f1.setPartitionFormat("date={year}-{month}-{day}");

        Feature f2 = new Feature();
        f2.setFeatureId(2L);
        f2.setFeatureName("User Profiles");
        f2.setFeatureLocation(FeatureLocation.AmazonS3);
        f2.setFeatureType(FeatureType.TSV);
        f2.setFeaturePath("s3://thetradedesk-useast-hadoop/unified-data-models/prod/site-summaries/");
        // TODO: we need to support different partition formats for different external features - this is just an example
        f2.setPartitionFormat("{year}/{month}/{day}");
        // TODO: We also need to be able to specify a schema for formats that don't contain one
        // e.g. tsv without headers

        TestFeatures.put(f1.getFeatureId(),f1);
        TestFeatures.put(f2.getFeatureId(),f2);
    }

    @GetMapping("/features")
    List<Feature> listFeatures()
    {
        return new ArrayList<>(TestFeatures.values());
    }

    @PostMapping("/features")
    Feature addFeature(@RequestBody Feature newFeature) {
        // TODO: add to DB and update feature to include feature id

        Optional<Long> maxFeatureId = TestFeatures.keySet().stream().max(Long::compareTo);
        long newFeatureId = maxFeatureId.map(aLong -> aLong + 1).orElse(1L);
        newFeature.setFeatureId(newFeatureId);
        TestFeatures.put(newFeature.getFeatureId(), newFeature);
        return newFeature;
    }

    @GetMapping("/features/{featureId}")
    Feature getFeature(@PathVariable Long featureId) {
        // TODO: Read feature info from the DB and return a Feature object
        return TestFeatures.get(featureId);
    }
}
