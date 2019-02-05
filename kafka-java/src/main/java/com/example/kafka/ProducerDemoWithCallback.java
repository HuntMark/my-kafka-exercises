package com.example.kafka;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.Properties;

public class ProducerDemoWithCallback {

    public static void main(String[] args) {

        Properties properties = new Properties();
        properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "127.0.0.1:9092");
        properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

        KafkaProducer<String, String> producer = new KafkaProducer<>(properties);

        ProducerRecord<String, String> record = new ProducerRecord<>("my-topic", "Hello World with callback!");

        for (int i = 0; i < 10; i++) {
            producer.send(record, (metadata, exception) -> {
                if (exception == null) {
                    System.out.println("Topic: " + metadata.topic()
                            + "\n Partition: " + metadata.partition()
                            + "\n Offset: " + metadata.offset()
                            + "\n Timestamp: " + metadata.timestamp());
                } else {
                    exception.printStackTrace();
                }
            });
            producer.flush();
        }


        producer.close();
    }
}
