package ru.t1.java.demo.controller.kafka.transaction;

import com.fasterxml.jackson.core.JsonProcessingException;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;
import ru.t1.java.demo.service.KafkaConsumerProcessorService;

@RestController
@RequestMapping("/ktc")
public class KafkaTransactionConsumer {

    private final KafkaConsumerProcessorService kafkaConsumerProcessorService;

    KafkaTransactionConsumer(KafkaConsumerProcessorService kafkaConsumerProcessorService){
        this.kafkaConsumerProcessorService = kafkaConsumerProcessorService;
    }

    @GetMapping("/cont")
    public ResponseEntity<String> getStringTransactionConsumer() throws JsonProcessingException {
        return new ResponseEntity<>(kafkaConsumerProcessorService.t1DemoTransactionConsumer(), HttpStatus.OK);

    }
    @GetMapping("/conta")
    public ResponseEntity<String> listenerTransactionAccept() throws JsonProcessingException {
        return new ResponseEntity<>(kafkaConsumerProcessorService.listenerTransactionAccept(), HttpStatus.OK);
    }
}
