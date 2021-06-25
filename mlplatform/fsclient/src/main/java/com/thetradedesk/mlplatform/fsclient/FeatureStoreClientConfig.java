package com.thetradedesk.mlplatform.fsclient;

import java.io.Serializable;

public class FeatureStoreClientConfig implements Serializable
{
    private String APIPath = "https://mlplatform.dev.gen.adsrvr.org";
    private String AWSAccessKey = null;
    private String AWSSecretKey = null;

    public String getAPIPath() {
        return APIPath;
    }

    public void setAPIPath(String APIPath) {
        this.APIPath = APIPath;
    }

    public String getAWSAccessKey() {
        return AWSAccessKey;
    }

    public void setAWSAccessKey(String AWSAccessKey) {
        this.AWSAccessKey = AWSAccessKey;
    }

    public String getAWSSecretKey() {
        return AWSSecretKey;
    }

    public void setAWSSecretKey(String AWSSecretKey) {
        this.AWSSecretKey = AWSSecretKey;
    }

    public FeatureStoreClientConfig()
    {

    }


}
