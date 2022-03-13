package com.cgroup.synclinkrequest.core;

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

    public LinkRequest(String httpMethod) {
        this(httpMethod, null, null);
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

    public void setUri(String uri) {
        this.uri = uri;
    }

    /**
     * 发送请求之前可以根据上一个请求的返回值，对uri和jsonContent做参数动态调整。
     *
     * @param currLinkRequest 当前的请求对象
     * @param preLinkResponse 前一个请求的返回结果
     */
    protected void processPreRequest(LinkRequest currLinkRequest, LinkResponse preLinkResponse) {

    }

    /**
     * 当出现IOException时，如果是流程内允许出现的，可通过该方法设置回LinkResponseState.Success
     *
     * @param exceptionMsg
     * @param preLinkResponse
     * @return
     */
    protected LinkResponseState processIOException(String exceptionMsg, LinkResponse preLinkResponse) {
        return LinkResponseState.Exception;
    }

    /**
     * es-api返回结果处理方法
     *
     * @param currData        当使用低级别api执行时，currData为当前模版执行流程返回的结果，如果在自定义流程中，则不关心该值
     * @param preLinkResponse
     * @return
     */
    protected abstract LinkResponseState processResponse(String currData, LinkResponse preLinkResponse);

    public LinkResponse exec(RestClient restClient, LinkRequest currLinkRequest, LinkResponse preLinkResponse) {
        LinkResponse res = new LinkResponse();
        //为响应体添加请求信息
        res.setRequestInfo(buildRequestInfo(currLinkRequest));
        res.setState(LinkResponseState.None);
        processPreRequest(this, preLinkResponse);
        //如果三个参数均为空，则执行doExec方法，可灵活自定义es请求代码
        if (StringUtils.isBlank(httpMethod)
                && StringUtils.isBlank(uri)
                && StringUtils.isBlank(jsonContent)) {
            res.setState(processResponse("", preLinkResponse));
            return res;
        }

        Request request = new Request(httpMethod, uri);
        request.setJsonEntity(jsonContent);
        try {
            Response response = restClient.performRequest(request);
            HttpEntity entity = response.getEntity();
            String data = EntityUtils.toString(entity);
            res.setData(data);
            res.setState(processResponse(data, preLinkResponse));
        } catch (IOException e) {
            res.setState(LinkResponseState.Exception);
            String exceptionMsg = "exception".concat("|").concat(e.getMessage());
            res.setData(exceptionMsg);
            res.setState(processIOException(exceptionMsg, preLinkResponse));
        }
        return res;
    }

    private String buildRequestInfo(LinkRequest currLinkRequest) {
        StringBuilder info = new StringBuilder();
        info.append("[".concat(currLinkRequest.getHttpMethod() == null ? "" : currLinkRequest.getHttpMethod()).concat("]"));
        info.append("\n uri=".concat(currLinkRequest.getUri() == null ? "" : currLinkRequest.getUri()));
        info.append("\n jsonContent=".concat(currLinkRequest.getJsonContent() == null ? "" : currLinkRequest.getJsonContent()));
        return info.toString();
    }
}
