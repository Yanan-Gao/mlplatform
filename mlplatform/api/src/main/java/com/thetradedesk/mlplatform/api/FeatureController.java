package com.thetradedesk.mlplatform.api;

import com.thetradedesk.mlplatform.common.featurestore.Feature;
import com.thetradedesk.mlplatform.common.featurestore.TestFeatures;
import io.prometheus.client.SimpleTimer;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.springframework.web.bind.annotation.*;

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

@RestController
public class FeatureController
{
    public static final Logger log  = LogManager.getLogger(FeatureController.class);

    private void IncrementRequestsCounter(String requestType)
    {
        RestService.RequestsCounter.labels("feature_controller",requestType, RestService.Config.Environment).inc();
    }

    private void IncrementRequestLatencySummary(String requestType, SimpleTimer timer)
    {
        RestService.RequestLatencyMs.labels("feature_controller",requestType, RestService.Config.Environment).observe(timer.elapsedSeconds() * 1000.0);
    }

    @GetMapping("/features")
    List<Feature> listFeatures()
    {
        SimpleTimer timer = new SimpleTimer();
        try {
            return new ArrayList<>(TestFeatures.Features.values());
        } finally {
            this.IncrementRequestLatencySummary("list_features", timer);
            this.IncrementRequestsCounter("list_features");
        }
    }

    @PostMapping("/features")
    Feature addFeature(@RequestBody Feature newFeature) {
        // TODO: add to DB and update feature to include feature id
        SimpleTimer timer = new SimpleTimer();
        try {
            Optional<Long> maxFeatureId = TestFeatures.Features.keySet().stream().max(Long::compareTo);
            long newFeatureId = maxFeatureId.map(aLong -> aLong + 1).orElse(1L);
            newFeature.setFeatureId(newFeatureId);
            TestFeatures.Features.put(newFeature.getFeatureId(), newFeature);
            return newFeature;
        } finally {
            this.IncrementRequestLatencySummary("add_feature", timer);
            this.IncrementRequestsCounter("add_feature");
        }
    }

    @GetMapping("/features/{featureId}")
    Feature getFeature(@PathVariable Long featureId)
    {
        SimpleTimer timer = new SimpleTimer();
        try {
            // TODO: Read feature info from the DB and return a Feature object
            Feature response = TestFeatures.Features.get(featureId);
            if (response == null) {
                log.error(String.format("Request for feature id %d failed - feature was not found", featureId));
            }
            return response;
        } finally {
            this.IncrementRequestLatencySummary("get_feature", timer);
            this.IncrementRequestsCounter("get_feature");
        }
    }
}
