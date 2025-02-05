package ru.t1.java.demo.service.kafkaProducer.topics;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.Getter;
import lombok.Setter;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;
import ru.t1.java.demo.enums.transaction.StatusTransactions;
import ru.t1.java.demo.service.kafkaProducer.ProcessingConsumerData;

@Getter
@Setter
@Service
public class T1DemoTransactionBlocked {

    private Integer clientId;
    private Integer transactionId;
    private String status;
    private ProcessingConsumerData processingConsumerData;

    @Autowired
    T1DemoTransactionBlocked(ProcessingConsumerData processingConsumerData){
        this.processingConsumerData = processingConsumerData;
    }
    T1DemoTransactionBlocked(){

    }
    public void setT1DemoTransactionBlocked(String statusTransactions, T1DemoTransactionAccept t1DemoTransactionAccept){
        T1DemoTransactionBlocked t1DemoTransactionBlocked = new T1DemoTransactionBlocked();
        t1DemoTransactionBlocked.setClientId(t1DemoTransactionAccept.getClientId());
        t1DemoTransactionBlocked.setTransactionId(t1DemoTransactionAccept.getTransactionId());
        t1DemoTransactionBlocked.setStatus(statusTransactions);

        ObjectMapper objectMapper = new ObjectMapper();
        String json;
        try {
            json  = objectMapper.writeValueAsString(t1DemoTransactionBlocked);
        } catch (JsonProcessingException e) {
            throw new RuntimeException(e);
        }
        processingConsumerData.setDataToTopic("t1_demo_transaction_result", json);
    }

}
