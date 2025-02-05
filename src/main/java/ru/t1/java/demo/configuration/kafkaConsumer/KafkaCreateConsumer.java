package ru.t1.java.demo.configuration.kafkaConsumer;

import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.*;
import ru.t1.java.demo.configuration.kafkaConfigConsumer.KafkaConfigConsumer;

@Configuration
public class KafkaCreateConsumer {

    private final KafkaConfigConsumer kafkaConfigConsumer;

    @Autowired
    KafkaCreateConsumer(KafkaConfigConsumer kafkaConfigConsumer){
        this.kafkaConfigConsumer = kafkaConfigConsumer;
    }

    @Bean
    public KafkaConsumer<String, String> getKafkaConsumer(){
        return new KafkaConsumer<>(kafkaConfigConsumer.consumerProperties());
    }
}
