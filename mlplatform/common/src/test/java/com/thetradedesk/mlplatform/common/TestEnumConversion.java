package com.thetradedesk.mlplatform.common;

import com.thetradedesk.mlplatform.common.featurestore.FeatureLocation;
import com.thetradedesk.mlplatform.common.featurestore.FeatureType;
import org.junit.Assert;
import org.junit.Test;

public class TestEnumConversion
{
    @Test
    public void TestFeatureLocation()
    {
        for(FeatureLocation source : FeatureLocation.values())
        {
            int sourceValue = source.getValue();
            FeatureLocation newSource = FeatureLocation.fromInt(sourceValue);
            // if this throws you forgot to add an int value to the fromInt switch statement
            Assert.assertEquals(source, newSource);
        }
    }

    @Test
    public void TestFeatureType()
    {
        for(FeatureType source : FeatureType.values())
        {
            int sourceValue = source.getValue();
            FeatureType newSource = FeatureType.fromInt(sourceValue);
            // if this throws you forgot to add an int value to the fromInt switch statement
            Assert.assertEquals(source, newSource);
        }
    }
}
