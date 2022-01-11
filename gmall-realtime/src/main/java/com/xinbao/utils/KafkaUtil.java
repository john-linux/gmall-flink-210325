package com.xinbao.utils;

import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer;
import org.apache.kafka.clients.consumer.ConsumerConfig;

import java.util.Properties;

public class KafkaUtil {

    private static String brokers = "hadoop102:9092,hadoop103:9092,hadoop104:9092";
    private static String default_topic = "DWD_DEFAULT_TOPIC";

    public static FlinkKafkaConsumer<String> getKafkaConsumer (String topic ,String groupId){
        Properties properties = new Properties();
        properties.put(ConsumerConfig.GROUP_ID_CONFIG,groupId);
        properties.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG,brokers);
        return new FlinkKafkaConsumer<String>(topic,new SimpleStringSchema(),properties);
    }
    public static FlinkKafkaProducer<String> getKafkaProducer(String topic) {
        return new FlinkKafkaProducer<String>(brokers,
                topic,
                new SimpleStringSchema());
    }



}
