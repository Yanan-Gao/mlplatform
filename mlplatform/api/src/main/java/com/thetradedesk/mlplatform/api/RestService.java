package com.thetradedesk.mlplatform.api;

import io.prometheus.client.Counter;
import io.prometheus.client.exporter.HTTPServer;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

import java.io.IOException;

@SpringBootApplication
public class RestService
{
    public static final Counter RequestsCounter = Counter.build()
            .name("mlplatform_api_requests")
            .help("Total requests sent to the mlplatform api.")
            .labelNames("controller","request", "env").register();

    public static RestServiceConfig Config;

    public static HTTPServer MetricsServer;

    public static void main(String[] args) throws IOException
    {
        // build config from env variables
        RestService.Config = RestServiceConfig.fromEnv();

        // build prometheus web service
        RestService.MetricsServer = new HTTPServer( RestService.Config.MetricsPort );

        SpringApplication.run(RestService.class, args);

    }

}
