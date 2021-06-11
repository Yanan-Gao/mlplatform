package com.thetradedesk.mlplatform.api;

import java.util.concurrent.atomic.AtomicLong;

import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

@RestController
public class APITestController
{
    private static final String template = "You passed %s!";
    private final AtomicLong counter = new AtomicLong();

    @GetMapping("/testcall")
    public APITestResponse TestCall(
            @RequestParam(value = "stringVar", defaultValue = "test")
            String stringVar
    )
    {
        return new APITestResponse(counter.incrementAndGet(), String.format(template, stringVar));
    }
}
