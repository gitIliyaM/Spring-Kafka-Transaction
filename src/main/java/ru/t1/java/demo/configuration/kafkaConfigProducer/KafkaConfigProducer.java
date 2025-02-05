package ru.t1.java.demo.configuration.kafkaConfigProducer;

import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.StringSerializer;
import org.springframework.context.annotation.*;
import java.util.*;

@Configuration
public class KafkaConfigProducer {

    @Bean
    public Properties producerProperties() {
        return setProperties();
    }

    public Properties setProperties() {
        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        //props.put(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, "false");
        //props.put(ProducerConfig.RETRIES_CONFIG, "5");
        props.put(ProducerConfig.ACKS_CONFIG, "0");
        return props;
    }
}
