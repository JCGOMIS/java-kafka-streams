package com.jcgomis.javakafkastreams.executorService;

import org.apache.kafka.clients.consumer.ConsumerRecord;

import java.util.Arrays;
import java.util.Properties;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

public class ConsumerKafkaMuliThread {
    public static void main(String[] args) {
        Properties props = new Properties();

        props.put("bootstrap.servers","localhost:9092");
        props.put("group.id", "test-group-id");
        props.put("enable.auto.commit","true");
        props.put("auto.commit.ms","1000");
        props.put("key.deserializer","org.apache.kafka.common.serialization.StringDeserializer");
        props.put("value.deserializer","org.apache.kafka.common.serialization.StringDeserializer");

         BasicConsumeLoop basicConsumer1 = new BasicConsumeLoop(props, Arrays.asList("jcMultithread")) {
            @Override
            public void process(ConsumerRecord record) {
                System.out.printf("Consumer 1 - offset = %d , key = %s - value = %s%n", record.offset(), record.key(), record.value());

            }
        };
          BasicConsumeLoop basicConsumer2 = new BasicConsumeLoop(props, Arrays.asList("jcMultithread")) {
            @Override
            public void process(ConsumerRecord record) {
                System.out.printf("Consumer 2 - offset = %d , key = %s - value = %s%n", record.offset(), record.key(), record.value());

            }
        };
        ExecutorService executorService =  Executors.newFixedThreadPool(2);
        executorService.execute(basicConsumer1);
        executorService.execute(basicConsumer2);

    }
}
