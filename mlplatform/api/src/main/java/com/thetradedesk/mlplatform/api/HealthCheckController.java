package com.thetradedesk.mlplatform.api;

import io.prometheus.client.SimpleTimer;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

@RestController
public class HealthCheckController
{
    public static final Logger log  = LogManager.getLogger(HealthCheckController.class);

    private void IncrementRequestsCounter(String requestType)
    {
        RestService.RequestsCounter.labels("health_check_controller",requestType, RestService.Config.Environment).inc();
    }

    private void IncrementRequestLatencySummary(String requestType, SimpleTimer timer)
    {
        RestService.RequestLatencyMs.labels("health_check_controller",requestType, RestService.Config.Environment).observe(timer.elapsedSeconds() * 1000.0);
    }

    @RequestMapping(value = "/healthcheck", method = RequestMethod.GET)
    @ResponseBody
    public ResponseEntity healthCheck()
    {
        SimpleTimer timer = new SimpleTimer();
        try {
            // Any code greater than or equal to 200 and less than 400 indicates success. Any other code indicates failure.
            // TODO: Check any important metrics you might care about here and return a different httpstatus

            // TODO: if you want to log issues with the health check, use the following
            // log.error("Error description", <optional Exception>);

            return new ResponseEntity(HttpStatus.OK);
        }
        finally
        {
            IncrementRequestLatencySummary("health_check", timer);
            IncrementRequestsCounter("health_check");
        }
    }
}
