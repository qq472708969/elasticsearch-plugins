package com.kafka;

import org.apache.kafka.clients.producer.Producer;

import java.util.List;

/**
 * Created by zzq on 2022/4/19/019.
 */
public class KafkaConnectionInfo extends KafkaConnectionParams {
    private Producer<String, byte[]> producer;

    public KafkaConnectionInfo() {

    }

    public KafkaConnectionInfo(KafkaConnectionParams kafkaConnectionParams) {
        super.setHost(kafkaConnectionParams.getHost());
        super.setName(kafkaConnectionParams.getName());
        super.setHost(kafkaConnectionParams.getHost());
    }

    public Producer<String, byte[]> getProducer() {
        return producer;
    }

    public void setProducer(Producer<String, byte[]> producer) {
        this.producer = producer;
    }
}
