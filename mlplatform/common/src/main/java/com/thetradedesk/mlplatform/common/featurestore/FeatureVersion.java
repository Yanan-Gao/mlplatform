package com.thetradedesk.mlplatform.common.featurestore;

import java.io.Serializable;
import java.util.List;
import java.util.Date;
import com.thetradedesk.mlplatform.common.featurestore.FeatureDataset;

public class FeatureVersion implements Serializable
{
    private Long featureId;
    private Long featureVersion;
    private String codePath;
    private String dataPath;
    private String documentationPath;
    private String schemaString;

    public FeatureVersion(){};

    public FeatureVersion(long featureId, long featureVersion, String codePath, String dataPath, String documentationPath, String schemaString) {
        this.featureId = featureId;
        this.featureVersion = featureVersion;
        this.codePath = codePath;
        this.dataPath = dataPath;
        this.documentationPath = documentationPath;
        this.schemaString = schemaString;
    }

    public Long getFeatureId() {
        return this.featureId;
    }
    public void setFeatureId(Long featureId) {
        this.featureId = featureId;
    }

    public Long getFeatureVersion() {
        return this.featureVersion;
    }
    public void setFeatureVersion(Long featureVersionId) {
        this.featureVersion = featureVersion;
    }

    public String getCodePath() {
        return this.codePath;
    }
    public void setCodePath(String codePath) {
        this.codePath = codePath;
    }

    public String getDataPath() {
        return this.dataPath;
    }
    public void setDataPath(String dataPath) { this.dataPath = dataPath; }

    public String getDocumentationPath() {
        return this.documentationPath;
    }
    public void setDocumentationPath(String documentationPath) { this.documentationPath = documentationPath; }

    public String getSchemaString() {
        return this.schemaString;
    }
    public void setSchemaString(String schemaString) { this.schemaString = schemaString; }
}
