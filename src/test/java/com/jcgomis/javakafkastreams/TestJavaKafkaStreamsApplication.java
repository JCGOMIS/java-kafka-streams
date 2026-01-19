package com.jcgomis.javakafkastreams;

import org.springframework.boot.SpringApplication;

public class TestJavaKafkaStreamsApplication {

    public static void main(String[] args) {
        SpringApplication.from(JavaKafkaStreamsApplication::main).with(TestcontainersConfiguration.class).run(args);
    }

}
