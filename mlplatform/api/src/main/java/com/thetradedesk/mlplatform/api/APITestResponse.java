package com.thetradedesk.mlplatform.api;

public class APITestResponse
{
    private final long id;
    private final String content;

    public APITestResponse(long id, String content) {
        this.id = id;
        this.content = content;
    }

    public long getId() {
        return id;
    }

    public String getContent() {
        return content;
    }
}
