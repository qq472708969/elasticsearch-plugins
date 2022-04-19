package com.kafka;

import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.util.ArrayList;
import java.util.List;

/**
 * Created by zzq on 2022/4/18/018.
 */
public class KafkaManager {
    private static KafkaClients clients;

    static {
        List<KafkaConnectionParams> connectionInfo = new ArrayList<KafkaConnectionParams>(3) {{
            add(new KafkaConnectionParams("za", new ArrayList<>(), "topicName"));
            add(new KafkaConnectionParams("zav3", new ArrayList<>(), "topicName"));
            add(new KafkaConnectionParams("apm", new ArrayList<>(), "topicName"));
        }};
        clients = KafkaClients.init(connectionInfo);
    }

    /**
     * 向kafka发送数据
     *
     * @param key   应用名标识
     * @param value 向生产者发送的byte[]数据
     * @return
     */
    public static boolean send(String key, byte[] value) {
        KafkaConnectionInfo kafkaConnectionInfo = clients.getKafkaConnectionInfo(key);
        Producer<String, byte[]> producer = kafkaConnectionInfo.getProducer();
        String topicName = kafkaConnectionInfo.getTopicName();
//        int partitionCount = producer.partitionsFor(topicName).size();
        producer.send(new ProducerRecord<>(kafkaConnectionInfo.getTopicName(), null, value));
        return true;
    }
}
