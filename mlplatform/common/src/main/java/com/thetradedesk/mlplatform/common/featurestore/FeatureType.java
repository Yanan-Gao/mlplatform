package com.thetradedesk.mlplatform.common.featurestore;

import java.io.Serializable;

public enum FeatureType  implements Serializable
{
    Unknown(0),
    Parquet(1),
    TSV(2);

    private final int value;

    FeatureType(int value) { this.value = value; }

    public int getValue() { return this.value; }

    public static FeatureType fromInt(int i)
    {
        switch (i)
        {
            case 1:
                return Parquet;
            case 2:
                return TSV;
            default:
                return Unknown;
        }
    }
}
