package com.atguigu.producer;

import org.apache.kafka.clients.producer.*;

import java.util.Properties;

public class CustomerProducerWithCallBack {

    public static void main(String[] args) {

        //1.创建配置信息
        Properties properties = new Properties();
        properties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "hadoop103:9092");
        properties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer");
        properties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer");

        properties.put(ProducerConfig.PARTITIONER_CLASS_CONFIG, "com.atguigu.partitioner.CustomerPartitioner");

        //2.创建生产者对象
        KafkaProducer<String, String> producer = new KafkaProducer<>(properties);

        //3.循环发送数据
        for (int i = 0; i < 10; i++) {

            producer.send(new ProducerRecord<>("first", "atguigu" + i, "atguigu" + i), (metadata, exception) -> {

                if (exception == null) {
                    System.out.println(metadata.topic() + "---" + metadata.partition() + "---" + metadata.offset());
                }
            });
        }

        //4.关闭连接
        producer.close();

    }

}
