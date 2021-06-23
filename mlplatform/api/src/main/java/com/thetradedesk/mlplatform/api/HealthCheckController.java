package com.thetradedesk.mlplatform.api;

import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

@RestController
public class HealthCheckController
{
    @RequestMapping(value = "/healthcheck", method = RequestMethod.GET)
    @ResponseBody
    public ResponseEntity healthCheck()
    {
        // Any code greater than or equal to 200 and less than 400 indicates success. Any other code indicates failure.
        // TODO: Check any important metrics you might care about here and return a different httpstatus
        return new ResponseEntity(HttpStatus.OK);
    }
}
