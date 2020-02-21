package com.atguigu.consumer;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;

import java.util.Collections;
import java.util.Properties;

public class CustomerConsumer {

    public static void main(String[] args) {

        //1.创建配置信息
        Properties props = new Properties();

        //指定连接的集群信息
        props.put("bootstrap.servers", "hadoop102:9092");

        //组id
        props.put("group.id", "test");
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");

        //设置自动提交
        props.put("enable.auto.commit", "false");
        //自动提交的延时
//        props.put("auto.commit.interval.ms", "2000");

        //Key Value的反序列化的类
        props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        props.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");

        //2.创建消费者对象
        KafkaConsumer<String, String> consumer = new KafkaConsumer<>(props);

        //3.订阅主题
        consumer.subscribe(Collections.singletonList("error"));

        //4.消费数据
        while (true) {

            //拉取一次数据
            ConsumerRecords<String, String> consumerRecords = consumer.poll(1000);

            //consumerRecords处理
            for (ConsumerRecord<String, String> consumerRecord : consumerRecords) {

                //打印一条数据
                System.out.println("Topic:" + consumerRecord.topic()
                        + "，Partition:" + consumerRecord.partition()
                        + "，Offset:" + consumerRecord.offset()
                        + "，Key:" + consumerRecord.key()
                        + "，Value:" + consumerRecord.value());
            }

            consumer.commitAsync();
        }

    }
}
