package com.thetradedesk.mlplatform.common.featurestore;

import java.io.Serializable;
import java.util.List;
import java.util.ArrayList;
import java.util.Date;

public class Feature implements Serializable
{
    private Long featureId;
    private String featureName;
    private String featureDescription;

    public Feature(){};

    public Feature(long featureId,
                   String featureName,
                   String featureDescription) {
        this.featureId = featureId;
        this.featureName = featureName;
        this.featureDescription = featureDescription;
    }

    public long getFeatureId() { return this.featureId; }
    public String getFeatureName() {
        return this.featureName;
    }
    public String getFeatureDescription() {
        return this.featureDescription;
    }
    public void setFeatureId(Long featureId) { this.featureId =  featureId; }
    public void setFeatureName(String featureName) { this.featureName = featureName; }
    public void setFeatureDescription(String featureDescription) { this.featureDescription = featureDescription; }

}
