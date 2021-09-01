package com.thetradedesk.mlplatform.common.featurestore;

import java.io.Serializable;
import java.util.List;
import java.util.ArrayList;
import java.util.Date;

public class FeatureTag implements Serializable
{
    private Long featureId;
    private String tagName;

    public FeatureTag(){};

    public FeatureTag(long featureId, String tagName) {
        this.featureId = featureId; //contains one featureId
        this.tagName = tagName;
    }

    public Long getFeatureId() {
        return this.featureId;
    }
    public void setFeatureId(Long featureId) { this.featureId = featureId; }

    public String getTagName() {
        return this.tagName;
    }
    public void setTagName(String tagName) {
        this.tagName = tagName;
    }

}
