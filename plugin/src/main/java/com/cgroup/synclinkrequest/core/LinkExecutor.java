package com.cgroup.synclinkrequest.core;

import org.apache.commons.collections.CollectionUtils;
import org.elasticsearch.client.RestClient;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.TimeUnit;

/**
 * Created by zzq on 2022/3/11.
 */
public class LinkExecutor {

    private List<LinkRequest> requests;
    private RestClient restClient;
    /**
     * 执行算子时，循环一次的间隔时间
     */
    private long sleepSecond;
    /**
     * 循环多少次无果后退出
     */
    private int repeatLimit;

    public LinkExecutor(RestClient restClient, List<LinkRequest> requests) {
        if (CollectionUtils.isEmpty(requests)) {
            return;
        }
        this.requests = requests;
        this.restClient = restClient;
        sleepSecond = 3L;
        repeatLimit = Integer.MAX_VALUE;
    }

    public LinkExecutor(RestClient restClient, LinkRequest... requests) {
        if (requests == null || requests.length == 0) {
            return;
        }
        this.requests = new ArrayList<>(Arrays.asList(requests));
        this.restClient = restClient;
        sleepSecond = 3L;
        repeatLimit = Integer.MAX_VALUE;
    }

    public void setSleepSecond(long second) {
        this.sleepSecond = second;
    }

    public void setRepeatLimit(int repeatLimit) {
        this.repeatLimit = repeatLimit - 1;
    }

    /**
     * RestClient执行低级别API
     *
     * @return
     */
    public List<LinkResponse> exec() {
        List<LinkResponse> resList = new ArrayList<>(5);
        LinkResponse tmp = new LinkResponse();
        for (int i = 0; i < requests.size(); i++) {
            LinkRequest linkRequest = requests.get(i);
            LinkResponse linkResponse = linkRequest.exec(restClient, linkRequest, tmp);
            //如果要求重新执行该算子，则外层循环暂停
            int times = 0;
            for (; linkResponse.getState().equals(LinkResponseState.Repeat)
                    && repeatLimit > 0
                    && times < repeatLimit; times++) {
                //不为初始值，则执行线程将进入即时等待状态
                if (sleepSecond > 0L) {
                    try {
                        TimeUnit.SECONDS.sleep(sleepSecond);
                    } catch (Exception e) {
                        //忽略中断异常，继续重试
                    }
                }
                linkResponse = linkRequest.exec(restClient, linkRequest, tmp);
            }
            resList.add(linkResponse);
            //缓存前一个执行器的结果
            tmp = linkResponse;
            //请求执行非正常情况，则直接停止
            if (!linkResponse.getState().equals(LinkResponseState.Success)) {
                break;
            }
        }
        return resList;
    }
}
