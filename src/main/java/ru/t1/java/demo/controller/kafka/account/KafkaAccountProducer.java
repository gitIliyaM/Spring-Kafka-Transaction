package ru.t1.java.demo.controller.kafka.account;

import org.springframework.beans.factory.annotation.*;
import org.springframework.http.*;
import org.springframework.web.bind.annotation.*;
import ru.t1.java.demo.dto.*;
import ru.t1.java.demo.model.*;
import ru.t1.java.demo.service.*;
import java.util.List;

@RestController
@RequestMapping("/kap")
public class KafkaAccountProducer {

    private final KafkaProducerProcessorService kafkaProducerProcessorService;

    @Autowired
    public KafkaAccountProducer(KafkaProducerProcessorService kafkaProducerProcessorService){
        this.kafkaProducerProcessorService = kafkaProducerProcessorService;
    }

    @GetMapping("/ga")
    public ResponseEntity<List<AccountDTO>> createAccountDTO() {
        return new ResponseEntity<>(kafkaProducerProcessorService.kafkaAccountDTOFromFile(), HttpStatus.OK);
    }
    @GetMapping("/gf")
    public ResponseEntity<List<Accounts>> createAccountFromFile() {
        return new ResponseEntity<>(kafkaProducerProcessorService.kafkaAccountFromFile(), HttpStatus.OK);
    }
    @PostMapping("/pra")
    public ResponseEntity<List<Accounts>> postAccountList(@RequestBody String account) {
        return new ResponseEntity<>(kafkaProducerProcessorService.kafkaAccountPost(account), HttpStatus.OK);
    }
}
