package com.jcgomis.javakafkastreams.executorService;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;

import java.util.List;
import java.util.Properties;
import java.util.concurrent.atomic.AtomicBoolean;

public  abstract class BasicConsumeLoop<K, V> implements Runnable {
    private final KafkaConsumer<K, V> consumer;
    private final List<String> topics;
    private final AtomicBoolean shutDown;

    public BasicConsumeLoop(Properties config, List<String> topics){
        this.consumer = new KafkaConsumer<>(config);
        this.topics = topics;
        this.shutDown = new AtomicBoolean(false);
    }
    public abstract void process(ConsumerRecord<K,V> record);

    @Override
    public void run() {
        try{
            consumer.subscribe(topics);
            while(!shutDown.get()){
                ConsumerRecords<K, V> records = consumer.poll(500);
                records.forEach(r->process(r)); //records.forEach(this::process);
            }
        }finally {
            consumer.close();
        }

    }
}
