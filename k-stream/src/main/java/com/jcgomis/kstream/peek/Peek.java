package com.jcgomis.kstream.peek;

import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.Produced;

import java.io.FileInputStream;
import java.io.IOException;
import java.util.Properties;
// Sert à des fins de debuggage
/*
 //La différrence entre peek et forEach est que forEach est une opération terminale et que peek
  est une opération intermédiaire on peut effectuer d'autres operation comme filter, map etc...

* */
public class Peek {
    public static void main(String[] args) throws IOException {
        Properties props = new Properties();
        try(FileInputStream fis = new FileInputStream("k-stream/src/main/resources/config/kstream.properties")){

            props.load(fis);
        }
        props.put(StreamsConfig.APPLICATION_ID_CONFIG,"basics-stream");

        final String INPUT_TOPIC = props.getProperty("input.topic");
        final String OUTPUT_TOPIC = props.getProperty("output.topic");

        final StreamsBuilder builder = new StreamsBuilder();
        KStream<String,String> source = builder.stream(INPUT_TOPIC);

        source.peek((k,v)->{
            System.out.printf("key %s : value %s \n", k, v);
        });

        source.to(OUTPUT_TOPIC, Produced.with(Serdes.String(), Serdes.String()));
        KafkaStreams kafkaStreams = new KafkaStreams(builder.build(),props);

        kafkaStreams.start();

    }
}
