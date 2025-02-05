package ru.t1.java.demo.controller.transactions;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.*;
import ru.t1.java.demo.dto.*;
import ru.t1.java.demo.model.*;
import ru.t1.java.demo.service.DataProcessorService;
import org.springframework.http.*;
import java.util.List;

@RestController
@RequestMapping("/rt")
public class TransactionClient {

    private final DataProcessorService dataProcessorService;

    @Autowired
    TransactionClient(DataProcessorService dataProcessorService){
        this.dataProcessorService = dataProcessorService;
    }
    // TransactionDTO[].class из File
    @GetMapping("/gt")
    private ResponseEntity<List<TransactionDTO>> getTransactionDTO() {
        return new ResponseEntity<>(dataProcessorService.createTransactionDTOFromFile(), HttpStatus.OK);
    }
    // Transactions.class из File
    @GetMapping("/gtx")
    public ResponseEntity<List<Transactions>> createTransactionFromFile()  {
        return new ResponseEntity<>(dataProcessorService.createTransactionFromFile(), HttpStatus.OK);
    }
    // Transactions.class из @PostMapping в String
    @PostMapping("/ptx")
    public ResponseEntity<List<Transactions>> postTransactionList(@RequestBody String transaction) {
        return new ResponseEntity<>(dataProcessorService.createTransactionPost(transaction), HttpStatus.OK);
    }
    @GetMapping("/gk")
    public ResponseEntity<List<TransactionKafkaDB>> createKafkaransactionFromFile()  {
        return new ResponseEntity<>(dataProcessorService.createKafkaTransactionFromFile(), HttpStatus.OK);
    }
}
