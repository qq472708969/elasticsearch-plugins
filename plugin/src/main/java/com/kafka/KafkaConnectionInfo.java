package com.kafka;

import java.util.List;

/**
 * Created by zzq on 2022/4/18/018.
 */
public class KafkaConnectionInfo {
    private String name;
    private List<String> host;
    private String topicName;

    public KafkaConnectionInfo(String name, List<String> host) {
        this.name = name;
        this.host = host;
    }

    public KafkaConnectionInfo(String name, List<String> host, String topicName) {
        this.name = name;
        this.host = host;
        this.topicName = topicName;
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public List<String> getHost() {
        return host;
    }

    public void setHost(List<String> host) {
        this.host = host;
    }

    public String getTopicName() {
        return topicName;
    }

    public void setTopicName(String topicName) {
        this.topicName = topicName;
    }
}
