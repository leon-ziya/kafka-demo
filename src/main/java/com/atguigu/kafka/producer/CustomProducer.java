package com.atguigu.kafka.producer;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.Properties;

/**
 * @author 姜来
 * @ClassName CustomProducer.java
 * @createTime 2022年11月11日 15:35:00
 */
public class CustomProducer {
    public static void main(String[] args) {
        // 1.创建配置对象
        Properties properties = new Properties();
        properties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG,"hadoop102:9092");
        properties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

        // 2. 创建kafka生产者对象
        KafkaProducer<String, String> producer = new KafkaProducer<String, String>(properties);
        // 3. 造点数据发送
        for(int i=1;i<11;i++){
            // 4. 造数据
            String message = "客观你好，我是"+i+"号，很高兴为你服务";
            // 5. 包装:创建对象produceRecord
            ProducerRecord<String, String> producerRecord = new ProducerRecord<String, String>(
                    "first",
                    0,
                    "",
                    message
            );
            System.out.println("你瞅瞅我在哪里？");

            // 6. 发送
            producer.send(producerRecord);
        }

        // 4. 关闭生产者
        producer.close();
    }
}
