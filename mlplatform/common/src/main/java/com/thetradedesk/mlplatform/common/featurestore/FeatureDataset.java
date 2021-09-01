package com.thetradedesk.mlplatform.common.featurestore;

import java.io.Serializable;
import java.util.List;
import java.util.ArrayList;
import java.util.Date;

public class FeatureDataset implements Serializable
{
    private long featureId;
    private long featureVersion;
    private Date featureProductionDate;
    private Date featureInputStart;
    private Date featureInputEnd;

    public FeatureDataset(){};

    public FeatureDataset(long featureId, long featureVersion, Date featureProductionDate, Date featureInputStart, Date featureInputEnd) {
        this.featureId = featureId;
        this.featureVersion = featureVersion;
        this.featureProductionDate = featureProductionDate;
        this.featureInputStart = featureInputStart;
        this.featureInputEnd = featureInputEnd;
    }

    public long getFeatureId() { return this.featureId; }
    public long getFeatureVersion() { return this.featureVersion; }
    public Date getFeatureProductionDate() {
        return this.featureProductionDate;
    }
    public Date getFeatureInputStart() {
        return this.featureInputStart;
    }
    public Date getFeatureInputEnd() {
        return this.featureInputEnd;
    }

    public void setFeatureId(long featureId) { this.featureId =  featureId; }
    public void setFeatureVersion(long featureVersion) { this.featureVersion =  featureVersion; }
    public void setFeatureProductionDate(Date featureProductionDate) { this.featureProductionDate =  featureProductionDate; }
    public void setFeatureInputStart(Date featureInputStart) { this.featureInputStart =  featureInputStart; }
    public void setFeatureInputEnd(Date featureInputEnd) { this.featureInputEnd =  featureInputEnd; }
}
