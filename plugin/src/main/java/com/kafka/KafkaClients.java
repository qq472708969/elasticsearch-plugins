package com.kafka;

import org.apache.commons.collections.CollectionUtils;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;

/**
 * Created by zzq on 2022/4/18/018.
 */
public class KafkaClients {
    private volatile static KafkaClients self;
    private Map<String, Producer<String, byte[]>> clients = new HashMap<>(3);
    private Map<String, KafkaConnectionInfo> connections = new HashMap<>(3);

    public static KafkaClients init(List<KafkaConnectionInfo> connectionInfo) {
        if (self == null) {
            synchronized (KafkaClients.class) {
                if (self == null) {
                    self = new KafkaClients(connectionInfo);
                }
            }
        }
        return self;
    }

    private KafkaClients(List<KafkaConnectionInfo> connectionInfo) {
        if (CollectionUtils.isEmpty(connectionInfo)) {
            throw new RuntimeException("connectionInfo is empty");
        }
        for (int i = 0; i < connectionInfo.size(); i++) {
            KafkaConnectionInfo kafkaConnectionInfo = connectionInfo.get(i);
            Producer<String, byte[]> producer = new KafkaProducer<>(getProperties(kafkaConnectionInfo.getHost()));
            clients.put(kafkaConnectionInfo.getName(), producer);
            connections.put(kafkaConnectionInfo.getName(), kafkaConnectionInfo);
        }
    }

    public Producer<String, byte[]> getClient(String key) {
        return clients.get(key);
    }

    public KafkaConnectionInfo getKafkaConnectionInfo(String key) {
        return connections.get(key);
    }

    private Properties getProperties(List<String> hosts) {
        Properties props = new Properties();
        if (CollectionUtils.isEmpty(hosts)) {
            return props;
        }
        // 必须指定 集群的连接地址，最好多个地址
        props.put("bootstrap.servers", String.join(",", hosts));
        // 必须指定 Key 的序列化器
        props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        // 必须指定 value 的序列化器
        props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        // 在请求完成之前，Producer 要求 Leader 收到的确认数。这将控制发送的记录的持久性。
        props.put("acks", "1");
        // 设置一个大于零的值将导致客户端重新发送其发送失败的任何记录
        props.put("retries", "3");
        // 每当多个记录被发送到同一分区时，生产者将尝试将记录批处理到一起，以减少请求。此配置控制以字节为单位的默认批处理大小。
        props.put("batch.size", "323840");
        // 一旦我们得到一个分区的batch.size值的记录，不管这个设置如何，它都会被立即发送，但是如果这个分区的累积字节数少于这个字节数，我们将“逗留”指定的时间，等待更多的记录出现
        props.put("linger.ms", "10");
        // Producer 可以用来缓冲等待发送到服务器的记录的总内存字节数
        props.put("buffer.memory", "33554432");
        // 此超时限制等待元数据获取和缓冲区分配的总时间
        props.put("max.block.ms", "3000");
        return props;
    }
}
