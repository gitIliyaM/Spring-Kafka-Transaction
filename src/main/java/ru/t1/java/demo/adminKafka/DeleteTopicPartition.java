package ru.t1.java.demo.adminKafka;

import org.apache.kafka.clients.admin.*;
import org.springframework.stereotype.*;
import java.util.concurrent.ExecutionException;
import java.util.*;

@Component
public class DeleteTopicPartition {

    public String deletePostTopic(String nameTopic) {
        Properties props = new Properties();
        props.put("bootstrap.servers", "localhost:9092");
        try (AdminClient adminClient = AdminClient.create(props)) {
            DeleteTopicsResult result = adminClient.deleteTopics(Collections.singletonList(nameTopic));
            // Блокирует поток до завершения удаления
            result.all().get();

            System.out.println("Топик " + nameTopic + " успешно удален.");
            return "Топик " + nameTopic + " успешно удален.";
        } catch (InterruptedException | ExecutionException e) {
            System.err.println("Ошибка при удалении топика: " + e.getMessage());
            return "Ошибка при удалении топика: " + e.getMessage();
        }
    }
}


