package ru.t1.java.demo.controller.accounts;

import org.springframework.beans.factory.annotation.Autowired;
import ru.t1.java.demo.service.DataProcessorService;
import org.springframework.web.bind.annotation.*;
import ru.t1.java.demo.dto.*;
import ru.t1.java.demo.model.*;
import org.springframework.http.*;
import java.util.List;

@RestController
@RequestMapping("/ra")
public class AccountClient {

    private final DataProcessorService dataProcessorService;

    @Autowired
    public AccountClient(DataProcessorService dataProcessorService){
        this.dataProcessorService = dataProcessorService;

    }
    // AccountDTO[].class из File
    @GetMapping("/ga")
    public ResponseEntity<List<AccountDTO>> getAccountDTO()  {
        return new ResponseEntity<>(dataProcessorService.createAccountDTOFromFile(), HttpStatus.OK);
    }
    // Accounts.class из File
    @GetMapping("/gf")
    public ResponseEntity<List<Accounts>> createAccountFromFile()  {
        return new ResponseEntity<>(dataProcessorService.createAccountFromFile(), HttpStatus.OK);
    }
    // Accounts.class из @PostMapping в String
    @PostMapping("/pa")
    public ResponseEntity<List<Accounts>> postAccountList(@RequestBody String account)  {
        return new ResponseEntity<>(dataProcessorService.createAccountPost(account), HttpStatus.OK);
    }
    @GetMapping("/gk")
    public ResponseEntity<List<AccountKafkaDB>> createKafkaAccountFromFile()  {
        return new ResponseEntity<>(dataProcessorService.createKafkaAccountFromFile(), HttpStatus.OK);
    }
}
