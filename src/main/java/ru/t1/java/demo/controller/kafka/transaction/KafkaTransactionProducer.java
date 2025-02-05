package ru.t1.java.demo.controller.kafka.transaction;

import org.springframework.beans.factory.annotation.*;
import org.springframework.http.*;
import org.springframework.web.bind.annotation.*;
import ru.t1.java.demo.dto.*;
import ru.t1.java.demo.model.*;
import ru.t1.java.demo.service.*;
import java.util.List;

@RestController
@RequestMapping("/ktp")
public class KafkaTransactionProducer {

    private final KafkaProducerProcessorService kafkaProducerProcessorService;

    @Autowired
    public KafkaTransactionProducer(KafkaProducerProcessorService kafkaProducerProcessorService){
        this.kafkaProducerProcessorService = kafkaProducerProcessorService;
    }

    @GetMapping("/gt")
    public ResponseEntity<List<TransactionDTO>> getTransactionDTO() {
        return new ResponseEntity<>(kafkaProducerProcessorService.kafkaTransactionDTOFromFile(), HttpStatus.OK);
    }
    @GetMapping("/gft")
    public ResponseEntity<List<Transactions>> createTransactionFromFile()  {
        return new ResponseEntity<>(kafkaProducerProcessorService.kafkaTransactionFromFile(), HttpStatus.OK);
    }
    @PostMapping("/prt")
    public ResponseEntity<List<Transactions>> postTransactionList(@RequestBody String transaction) {
        return new ResponseEntity<>(kafkaProducerProcessorService.kafkaTransactionPost(transaction), HttpStatus.OK);
    }
}
