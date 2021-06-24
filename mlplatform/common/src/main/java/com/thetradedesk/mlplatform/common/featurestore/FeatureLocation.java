package com.thetradedesk.mlplatform.common.featurestore;

import java.io.Serializable;

// TODO: these need to match the DB
public enum FeatureLocation implements Serializable
{
    Unknown(0),
    Local(1), // used for testing
    AmazonS3(2);

    private final int value;

    FeatureLocation(int value) { this.value = value; }

    public int getValue() { return this.value; }

    public static FeatureLocation fromInt(int i)
    {
        switch (i)
        {
            case 1:
                return Local;
            case 2:
                return AmazonS3;
            default:
                return Unknown;
        }
    }

}
