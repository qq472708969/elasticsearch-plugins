package com.cgroup.synclinkrequest;

import org.apache.http.HttpEntity;
import org.apache.http.util.EntityUtils;
import org.elasticsearch.client.Request;
import org.elasticsearch.client.Response;
import org.elasticsearch.client.RestClient;

import java.io.IOException;

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

    public LinkResponse execRequest(RestClient restClient) throws IOException {
        Request request = new Request(httpMethod, uri);
        request.setJsonEntity(jsonContent);
        Response response = restClient.performRequest(request);
        HttpEntity entity = response.getEntity();
        LinkResponse res = new LinkResponse();
        res.setState(ResLinkState.None);
        try {
            String ret = EntityUtils.toString(entity);
            res.setState(ResLinkState.Success);
            res.setData(ret);
            res.setMsg("success");
        } catch (IOException e) {
            res.setState(ResLinkState.Fail);
            res.setMsg("fail".concat(" | ").concat(e.getMessage()));
            e.printStackTrace();
        }
        return res;
    }
}
