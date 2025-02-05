package ru.t1.java.demo.configuration.kafkaProducer;

import ru.t1.java.demo.configuration.kafkaConfigProducer.KafkaConfigProducer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.*;


@Configuration
public class KafkaCreateProducer {

    private final KafkaConfigProducer kafkaConfigProducer;

    @Autowired
    KafkaCreateProducer(KafkaConfigProducer kafkaConfigProducer){
        this.kafkaConfigProducer = kafkaConfigProducer;
    }

    @Bean
    public KafkaProducer<String, String> getKafkaProducer(){
        return new KafkaProducer<>(kafkaConfigProducer.producerProperties());
    }
}
