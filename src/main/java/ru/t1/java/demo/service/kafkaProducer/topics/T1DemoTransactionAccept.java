package ru.t1.java.demo.service.kafkaProducer.topics;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.*;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.json.JSONObject;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;
import ru.t1.java.demo.enums.transaction.StatusTransactions;
import ru.t1.java.demo.model.AccountKafkaDB;
import ru.t1.java.demo.service.kafkaProducer.ProcessingConsumerData;

@Getter
@Setter
@Service
public class T1DemoTransactionAccept {

    private Integer clientId;
    private Integer accountId;
    private Integer transactionId;
    private Long timestamp;
    private Double transactionAmount;
    private Double accountBalance;
    private StatusTransactions status;
    private ProcessingConsumerData processingConsumerData;

    @Autowired
    T1DemoTransactionAccept(ProcessingConsumerData processingConsumerData){
        this.processingConsumerData = processingConsumerData;
    }

    public T1DemoTransactionAccept(Integer transactionId, Long timestamp, StatusTransactions status, Double transactionAmount) {
    }

    public T1DemoTransactionAccept(){

    }
    public void setT1DemoTransactionAccept(AccountKafkaDB accountKafkaDB, ConsumerRecord<String, String> consumerRecord, Long currentTimeMillis) {

        T1DemoTransactionAccept transactionAccept = new T1DemoTransactionAccept();
        transactionAccept.setClientId(accountKafkaDB.getIdClient());               // clientId
        transactionAccept.setAccountId(accountKafkaDB.getIdAccount());             // accountId
        transactionAccept.setTransactionId(new JSONObject(consumerRecord.value()).getInt("idTransaction"));            // transactionId
        transactionAccept.setTimestamp(currentTimeMillis);             // timestamp
        transactionAccept.setTransactionAmount(new JSONObject(consumerRecord.value()).getDouble("amountTransaction")); // transactionAmount
        transactionAccept.setAccountBalance(accountKafkaDB.getBalance());           // accountBalance

        ObjectMapper objectMapper = new ObjectMapper();
        String json;
        try {
            json  = objectMapper.writeValueAsString(transactionAccept);
        } catch (JsonProcessingException e) {
            throw new RuntimeException(e);
        }
        processingConsumerData.setDataToTopic("t1_demo_transaction_accept", json);
    }

}
