package com.thetradedesk.mlplatform.common.featurestore;

import java.io.Serializable;

public class Feature implements Serializable
{
    public Long FeatureId;
    public String FeatureName;
    public FeatureLocation FeatureLocation;
    public FeatureType FeatureType;
    public String FeaturePath;


}
