package ru.t1.java.demo.controller.kafka;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.*;
import org.springframework.web.bind.annotation.*;
import ru.t1.java.demo.adminKafka.*;

@RestController
@RequestMapping("/kafka")
public class KafkaClient {

    //Протестировать создание топика, выполните HTTP-запрос:
    // http://localhost:8080/kafka/create-topic?topicName=my-topic&partitions=3&replicas=1

    //Протестировать удаление топика, выполните HTTP-запрос:
    // http://localhost:8080/kafka/delete-topic/my-topic

    private final CreateTopicPartition createTP;
    private final DeleteTopicPartition deleteTP;

    @Autowired
    public KafkaClient(
        CreateTopicPartition createTP,
        DeleteTopicPartition deleteTP
    ){
        this.createTP = createTP;
        this.deleteTP = deleteTP;
    }
    @PostMapping("/create-topic")
    public ResponseEntity<String> createTopic(
        @RequestParam String topicName,
        @RequestParam int partitions,
        @RequestParam short replicas){
        return new ResponseEntity<>(createTP.createPostTopic(topicName, partitions, replicas), HttpStatus.OK);
    }
    @DeleteMapping("/delete-topic/{topicName}")
    public ResponseEntity<String> deleteTopic(@PathVariable String topicName){
        return new ResponseEntity<>(deleteTP.deletePostTopic(topicName), HttpStatus.OK);
    }
}
