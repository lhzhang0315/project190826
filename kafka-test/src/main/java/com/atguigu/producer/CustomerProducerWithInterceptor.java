package com.atguigu.producer;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.util.ArrayList;
import java.util.Properties;

public class CustomerProducerWithInterceptor {

    public static void main(String[] args) throws InterruptedException {

        //1.构建配置信息
        Properties properties = new Properties();
        properties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "hadoop102:9092");
        properties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer");
        properties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer");
        //发送一个批次的大小
        properties.put("batch.size", 16384);
        //发送延时时间
        properties.put("linger.ms", 1);

        //构建拦截器的List
        ArrayList<String> interceptors = new ArrayList<>();
        interceptors.add("com.atguigu.interceptor.TimeInterceptor");
        interceptors.add("com.atguigu.interceptor.CounterInterceptor");

        properties.put(ProducerConfig.INTERCEPTOR_CLASSES_CONFIG, interceptors);

        //2.创建生产者对象
        KafkaProducer<String, String> producer = new KafkaProducer<>(properties);

        //3.循环发送数据
        for (int i = 0; i < 10; i++) {
            producer.send(new ProducerRecord<>("first", "atguigu" + i));
        }

        //Thread.sleep(1000);

        //4.关闭连接
        producer.close();

    }

}
