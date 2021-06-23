package com.thetradedesk.mlplatform.api;

import com.thetradedesk.mlplatform.common.featurestore.Feature;
import com.thetradedesk.mlplatform.common.featurestore.TestFeatures;
import org.springframework.web.bind.annotation.*;

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

@RestController
public class FeatureController
{
    // TODO: Remove this and run everything from the DB - this is here for testing API calls prior to DB setup

    @GetMapping("/features")
    List<Feature> listFeatures()
    {
        return new ArrayList<>(TestFeatures.Features.values());
    }

    @PostMapping("/features")
    Feature addFeature(@RequestBody Feature newFeature) {
        // TODO: add to DB and update feature to include feature id

        Optional<Long> maxFeatureId = TestFeatures.Features.keySet().stream().max(Long::compareTo);
        long newFeatureId = maxFeatureId.map(aLong -> aLong + 1).orElse(1L);
        newFeature.setFeatureId(newFeatureId);
        TestFeatures.Features.put(newFeature.getFeatureId(), newFeature);
        return newFeature;
    }

    @GetMapping("/features/{featureId}")
    Feature getFeature(@PathVariable Long featureId) {
        // TODO: Read feature info from the DB and return a Feature object
        return TestFeatures.Features.get(featureId);
    }
}
