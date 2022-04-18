package com.kafka;

import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.PartitionInfo;

import java.util.ArrayList;
import java.util.List;

/**
 * Created by zzq on 2022/4/18/018.
 */
public class KafkaManager {
    private static KafkaClients clients;

    static {
        List<KafkaConnectionInfo> connectionInfo = new ArrayList<KafkaConnectionInfo>(3) {{
            add(new KafkaConnectionInfo("za", new ArrayList<>(), "topicName"));
            add(new KafkaConnectionInfo("zav3", new ArrayList<>(), "topicName"));
            add(new KafkaConnectionInfo("apm", new ArrayList<>(), "topicName"));
        }};
        clients = KafkaClients.init(connectionInfo);
    }

    public boolean send(String key) {
        Producer<String, byte[]> client = clients.getClient(key);
        KafkaConnectionInfo kafkaConnectionInfo = clients.getKafkaConnectionInfo(key);
        String topicName = kafkaConnectionInfo.getTopicName();
        int partitionCount = client.partitionsFor(topicName).size();

        client.send(new ProducerRecord<>(kafkaConnectionInfo.getTopicName(), ))
    }
}
