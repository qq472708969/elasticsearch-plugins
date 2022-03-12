package com.cgroup.synclinkrequest;

import org.apache.commons.lang3.StringUtils;
import org.apache.http.HttpEntity;
import org.apache.http.util.EntityUtils;
import org.elasticsearch.client.Request;
import org.elasticsearch.client.Response;
import org.elasticsearch.client.RestClient;

import java.io.IOException;

/**
 * Created by zzq on 2022/3/11.
 */
public abstract class LinkRequest {
    private String jsonContent;
    private String httpMethod;
    private String uri;

    public LinkRequest(String httpMethod, String uri, String jsonContent) {
        this.jsonContent = jsonContent;
        this.httpMethod = httpMethod;
        this.uri = uri;
    }

    public LinkRequest(String httpMethod, String uri) {
        this(httpMethod, uri, null);
    }

    public LinkRequest() {
    }

    public String getJsonContent() {
        return jsonContent;
    }

    public void setJsonContent(String jsonContent) {
        this.jsonContent = jsonContent;
    }

    public String getHttpMethod() {
        return httpMethod;
    }

    public String getUri() {
        return uri;
    }

    /**
     * es-api返回结果处理方法
     *
     * @param ret
     * @return
     */
    protected abstract LinkResponseState doExec(String ret);

    public LinkResponse exec(RestClient restClient) {
        LinkResponse res = new LinkResponse();
        res.setState(LinkResponseState.None);
        //如果三个参数均为空，则执行doExec方法，可灵活自定义es请求代码
        if (StringUtils.isBlank(httpMethod)
                && StringUtils.isBlank(uri)
                && StringUtils.isBlank(jsonContent)) {
            res.setState(doExec(null));
            return res;
        }

        Request request = new Request(httpMethod, uri);
        request.setJsonEntity(jsonContent);
        try {
            Response response = restClient.performRequest(request);
            HttpEntity entity = response.getEntity();
            String ret = EntityUtils.toString(entity);
            res.setData(ret);
            res.setState(doExec(ret));
        } catch (IOException e) {
            res.setState(LinkResponseState.Exception);
            res.setMsg("exception".concat("|").concat(e.getMessage()));
            e.printStackTrace();
        }
        return res;
    }
}
