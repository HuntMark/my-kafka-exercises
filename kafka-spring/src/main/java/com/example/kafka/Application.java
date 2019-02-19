package com.example.kafka;

import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.admin.NewTopic;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.CommandLineRunner;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.annotation.Bean;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.SendResult;
import org.springframework.util.concurrent.ListenableFuture;

import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;

@Slf4j
@SpringBootApplication
public class Application implements CommandLineRunner {

    public static void main(String[] args) {
        SpringApplication.run(Application.class, args).close();
    }

    private KafkaTemplate<Integer, String> kafkaTemplate;

    @Autowired
    public void setKafkaTemplate(KafkaTemplate<Integer, String> kafkaTemplate) {
        this.kafkaTemplate = kafkaTemplate;
    }

    @Override
    public void run(String... args) throws InterruptedException {
        for (int i = 0; i < 15; i++) {
            int key = ThreadLocalRandom.current().nextInt(1, 4);
            String message = "message #" + i + ", with key = " + key;
            TimeUnit.SECONDS.sleep(5);
            ListenableFuture<SendResult<Integer, String>> results = kafkaTemplate.send("topic1", key, message);
            results.addCallback(
                    result -> {
                        System.out.println("MESSAGE!!!");
                        System.out.println(result);
                    },
                    ex -> {
                        System.out.println("EXCEPTION!!!");
                        ex.printStackTrace();
                    }
            );
        }
    }

    @Bean
    public NewTopic topic1() {
        return new NewTopic("topic1", 3, (short) 3);
    }
}
