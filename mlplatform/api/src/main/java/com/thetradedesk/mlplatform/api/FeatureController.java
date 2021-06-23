package com.thetradedesk.mlplatform.api;

import com.thetradedesk.mlplatform.common.featurestore.Feature;
import com.thetradedesk.mlplatform.common.featurestore.TestFeatures;
import io.prometheus.client.Counter;
import org.springframework.web.bind.annotation.*;

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

@RestController
public class FeatureController
{
    private void IncrementRequestsCounter(String requestType)
    {
        RestService.RequestsCounter.labels("feature_controller",requestType, RestService.Config.Environment);
    }

    @GetMapping("/features")
    List<Feature> listFeatures()
    {
        this.IncrementRequestsCounter("list_features");
        return new ArrayList<>(TestFeatures.Features.values());

    }

    @PostMapping("/features")
    Feature addFeature(@RequestBody Feature newFeature) {
        // TODO: add to DB and update feature to include feature id

        this.IncrementRequestsCounter("add_feature");

        Optional<Long> maxFeatureId = TestFeatures.Features.keySet().stream().max(Long::compareTo);
        long newFeatureId = maxFeatureId.map(aLong -> aLong + 1).orElse(1L);
        newFeature.setFeatureId(newFeatureId);
        TestFeatures.Features.put(newFeature.getFeatureId(), newFeature);
        return newFeature;
    }

    @GetMapping("/features/{featureId}")
    Feature getFeature(@PathVariable Long featureId)
    {
        this.IncrementRequestsCounter("get_feature");
        // TODO: Read feature info from the DB and return a Feature object
        return TestFeatures.Features.get(featureId);
    }
}
