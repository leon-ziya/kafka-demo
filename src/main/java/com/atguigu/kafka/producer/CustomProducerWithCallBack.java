package com.atguigu.kafka.producer;

import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.Properties;

/**
 * @author 姜来
 * @ClassName CustomProducer.java
 * @createTime 2022年11月11日 15:35:00
 */
public class CustomProducerWithCallBack {
    public static void main(String[] args) throws InterruptedException {
        // 1.创建配置对象
        Properties properties = new Properties();
        properties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG,"hadoop102:9092");
        properties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

        // 配置自定义分区器
        properties.put(ProducerConfig.PARTITIONER_CLASS_CONFIG,"com.atguigu.kafka.partitioner.CustomPartitioner");

        // TODO 提高吞吐量
        // 1. 提高线程共享变量的大小
        properties.put(ProducerConfig.BUFFER_MEMORY_CONFIG,33554432);
        // 2. 提高批次大小
        properties.put(ProducerConfig.BATCH_SIZE_CONFIG,16384);
        properties.put(ProducerConfig.LINGER_MS_CONFIG,500);
        properties.put(ProducerConfig.COMPRESSION_TYPE_CONFIG,"snappy");

        // TODO 应答机制
        properties.put(ProducerConfig.ACKS_CONFIG,"all");
        properties.put(ProducerConfig.RETRIES_CONFIG,4);

        // TODO 幂等性的配置
        properties.put(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG,true);


        // 2. 创建kafka生产者对象
        KafkaProducer<String, String> producer = new KafkaProducer<String, String>(properties);
        // 3. 造点数据发送
        for(int i=1;i<11;i++){
            // 4. 造数据
            String message = "客观你好，我是"+i+"号，很高兴为你服务";
            // 5. 包装:创建对象produceRecord
            final ProducerRecord<String, String> producerRecord = new ProducerRecord<String, String>(
                    "first",
                    i+"",
                    message
            );
            //System.out.println("你瞅瞅我在哪里？");
            // 6. 发送
            Thread.sleep(500);
            producer.send(producerRecord, new Callback() {
                @Override
                public void onCompletion(RecordMetadata metadata, Exception exception) {
                    if(exception==null){
                        long offset = metadata.offset();
                        int partition = metadata.partition();
                        System.out.println("数据："+producerRecord.value()+"分区："+partition+", 偏移量："+offset);
                    }
                }
            });
        }

        // 4. 关闭生产者
        producer.close();
    }
}
