package com.cgroup.synclinkrequest;

/**
 * Created by zzq on 2022/3/11.
 */
public class LinkRequest {
    private String jsonContent;
    private String httpMethod;
    private String uri;

    public String getJsonContent() {
        return jsonContent;
    }

    public void setJsonContent(String jsonContent) {
        this.jsonContent = jsonContent;
    }

    public String getHttpMethod() {
        return httpMethod;
    }

    public void setHttpMethod(String httpMethod) {
        this.httpMethod = httpMethod;
    }

    public String getUri() {
        return uri;
    }

    public void setUri(String uri) {
        this.uri = uri;
    }
}
