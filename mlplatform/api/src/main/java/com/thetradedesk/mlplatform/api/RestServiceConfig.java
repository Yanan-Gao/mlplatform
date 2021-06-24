package com.thetradedesk.mlplatform.api;

import com.thetradedesk.mlplatform.common.util.EnvUtils;

import java.io.Serializable;
import java.util.Locale;

public class RestServiceConfig implements Serializable
{
    public final int MetricsPort;
    public final String Environment;

    public RestServiceConfig(int metricsPort, String environment)
    {
        this.MetricsPort = metricsPort;
        this.Environment = environment;
    }

    public static RestServiceConfig fromEnv()
    {
        int metricsPort = EnvUtils.GetInt("METRICS_PORT", 5002);
        String environment = EnvUtils.GetString("ENVIRONMENT", "test").toLowerCase();
        // TODO: Add other configs passed through kubernetes env variables here
        return new RestServiceConfig(
                metricsPort,
                environment
        );
    }

}
