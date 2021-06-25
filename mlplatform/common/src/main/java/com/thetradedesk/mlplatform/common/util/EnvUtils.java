package com.thetradedesk.mlplatform.common.util;

import java.util.Map;

public class EnvUtils
{
    static Map<String, String> EnvVariables = System.getenv();

    public static boolean GetBoolean(String key, Boolean defaultValue) throws IllegalArgumentException
    {
        String stringValue = EnvVariables.get(key);
        if(stringValue == null)
        {
            if(defaultValue != null)
            {
                return defaultValue;
            }
            throw new IllegalArgumentException(String.format("Environment variable %s not found", key));
        }
        return Boolean.parseBoolean(stringValue);
    }

    public static String GetString(String key, String defaultValue)
    {
        String stringValue = EnvVariables.get(key);
        if(stringValue == null)
        {
            return defaultValue;
        }
        return stringValue;
    }

    public static int GetInt(String key, Integer defaultValue) throws NumberFormatException, IllegalArgumentException
    {
        String stringValue = EnvVariables.get(key);
        if(stringValue == null)
        {
            // not found - if default value provided, return it
            if(defaultValue != null)
            {
                return defaultValue;
            }
            throw new IllegalArgumentException(String.format("Environment variable %s not found", key));
        }
        return Integer.parseInt(stringValue);
    }
}