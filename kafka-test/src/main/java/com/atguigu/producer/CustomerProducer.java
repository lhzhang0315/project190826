package com.atguigu.producer;

import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.util.Properties;
import java.util.concurrent.ExecutionException;

public class CustomerProducer {

    public static void main(String[] args) throws ExecutionException, InterruptedException {

        //1.构建配置信息
        Properties props = new Properties();

        //指定连接的Kafka集群
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "hadoop102:9092,hadoop103:9092,hadoop104:9092");

        //设置应答级别
        props.put(ProducerConfig.ACKS_CONFIG, "all");

        //重试次数
        props.put("retries", 3);

        //发送一个批次的大小
        props.put("batch.size", 16384);

        //发送延时时间
        props.put("linger.ms", 1);

        //缓冲区大小
        props.put("buffer.memory", 33554432);

        //指定Key和Value的序列化器
        props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");

        //2.创建Kafka生产者对象
        KafkaProducer<String, String> kafkaProducer = new KafkaProducer<>(props);

//        Thread.sleep(10);

        //3.发送数据
        long start = System.currentTimeMillis();
        for (int i = 0; i < 1000000; i++) {
            kafkaProducer.send(new ProducerRecord<>("first", "atguigu" + ":" + i));
        }
        long end = System.currentTimeMillis();

        System.out.println(end - start);

        //4.关闭连接
        kafkaProducer.close();
    }

}
