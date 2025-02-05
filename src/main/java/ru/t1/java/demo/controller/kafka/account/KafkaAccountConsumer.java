package ru.t1.java.demo.controller.kafka.account;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;
import ru.t1.java.demo.service.KafkaConsumerProcessorService;

@RestController
@RequestMapping("/kac")
public class KafkaAccountConsumer {

    private final KafkaConsumerProcessorService kafkaConsumerProcessorService;

    @Autowired
    KafkaAccountConsumer(KafkaConsumerProcessorService kafkaConsumerProcessorService){
        this.kafkaConsumerProcessorService = kafkaConsumerProcessorService;
    }

    @GetMapping("/cona")
    public ResponseEntity<String> getStringAccountConsumer(){
        return new ResponseEntity<>(kafkaConsumerProcessorService.t1DemoAccountsConsumer(), HttpStatus.OK);
    }
}
