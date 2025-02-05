package ru.t1.java.demo.adminKafka;

import org.apache.kafka.clients.admin.*;
import org.springframework.stereotype.*;
import java.util.concurrent.ExecutionException;
import java.util.*;

@Component
public class CreateTopicPartition {

    public String createPostTopic(String topicName, int partitions, short replicas){

        Properties props = new Properties();
        props.put("bootstrap.servers", "localhost:9092");

        try (AdminClient adminClient = AdminClient.create(props)){
            NewTopic newTopic = new NewTopic(topicName, partitions, replicas);
            // Блокирует поток до завершения создания
            adminClient.createTopics(Collections.singleton(newTopic)).all().get();

            System.out.println("Топик " + topicName + " успешно создан с " + partitions + " партициями.");
            return "Топик " + topicName + " успешно создан с " + partitions + " партициями.";
        } catch (InterruptedException | ExecutionException e) {
            System.err.println("Ошибка при создании топика: " + e.getMessage());
            return "Ошибка при создании топика: " + e.getMessage();
        }
    }
}
