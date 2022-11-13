package com.atguigu.kafka.partitioner;

import org.apache.kafka.clients.producer.Partitioner;
import org.apache.kafka.common.Cluster;

import java.util.Map;

/**
 * @author 姜来
 * @ClassName CustomPartitioner.java
 * @createTime 2022年11月12日 10:01:00
 */
public class CustomPartitioner implements Partitioner {
    /**
     * 实现分区
     * 对key进行我们常见的那种hash取值，数字就是本身的那种，
     * @param topic
     * @param key
     * @param keyBytes
     * @param value
     * @param valueBytes
     * @param cluster
     * @return
     */
    @Override
    public int partition(String topic, Object key, byte[] keyBytes, Object value, byte[] valueBytes, Cluster cluster) {
        // 1. 获取key的hash值
        String keyStr = key.toString();
        int hashCode = keyStr.hashCode();
        // 2. 获取分区个数
        Integer numPartition = cluster.partitionCountForTopic(topic);
        // 3. 计算分区号
        int partition = Math.abs(hashCode) % numPartition;

        return partition;
    }

    @Override
    public void close() {

    }

    @Override
    public void configure(Map<String, ?> configs) {

    }
}
