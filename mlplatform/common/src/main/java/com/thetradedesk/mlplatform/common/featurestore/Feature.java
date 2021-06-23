package com.thetradedesk.mlplatform.common.featurestore;

import java.io.Serializable;

// TODO: Make this match the DB
public class Feature implements Serializable
{
    public Long getFeatureId() {
        return FeatureId;
    }

    public void setFeatureId(Long featureId) {
        FeatureId = featureId;
    }

    public String getFeatureName() {
        return FeatureName;
    }

    public void setFeatureName(String featureName) {
        FeatureName = featureName;
    }

    public com.thetradedesk.mlplatform.common.featurestore.FeatureLocation getFeatureLocation() {
        return FeatureLocation;
    }

    public void setFeatureLocation(com.thetradedesk.mlplatform.common.featurestore.FeatureLocation featureLocation) {
        FeatureLocation = featureLocation;
    }

    public com.thetradedesk.mlplatform.common.featurestore.FeatureType getFeatureType() {
        return FeatureType;
    }

    public void setFeatureType(com.thetradedesk.mlplatform.common.featurestore.FeatureType featureType) {
        FeatureType = featureType;
    }

    public String getFeaturePath() {
        return FeaturePath;
    }

    public void setFeaturePath(String featurePath) {
        FeaturePath = featurePath;
    }

    public String getPartitionFormat() {
        return PartitionFormat;
    }

    public void setPartitionFormat(String partitionFormat) {
        PartitionFormat = partitionFormat;
    }

    public Long FeatureId;
    public String FeatureName;
    public FeatureLocation FeatureLocation;
    public FeatureType FeatureType;
    public String FeaturePath;
    public String PartitionFormat;


}
