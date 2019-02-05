package com.example.kafka;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.Properties;
import java.util.concurrent.ExecutionException;

public class ProducerDemoKeys {

    public static void main(String[] args) throws ExecutionException, InterruptedException {

        Properties properties = new Properties();
        properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "127.0.0.1:9092");
        properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

        KafkaProducer<String, String> producer = new KafkaProducer<>(properties);


        for (int i = 0; i < 10; i++) {

            String key = "id_" + i;

            // the same key always goes to the same partition!

            // id_0 -> partition 1
            // id_1 -> partition 0
            // id_2 -> partition 2
            // id_3 -> partition 0
            // id_4 -> partition 2
            // id_5 -> partition 2
            // id_6 -> partition 0
            // id_7 -> partition 2
            // id_8 -> partition 1
            // id_9 -> partition 2

            System.out.println(key);

            ProducerRecord<String, String> record = new ProducerRecord<>("my-topic", key, "Hello World with callback!");

            producer.send(record, (metadata, exception) -> {
                if (exception == null) {
                    System.out.println("Topic: " + metadata.topic()
                            + "\n Partition: " + metadata.partition()
                            + "\n Offset: " + metadata.offset()
                            + "\n Timestamp: " + metadata.timestamp());
                } else {
                    exception.printStackTrace();
                }
            }).get(); // don't do this on production!

        }

        producer.flush();

        producer.close();
    }
}
