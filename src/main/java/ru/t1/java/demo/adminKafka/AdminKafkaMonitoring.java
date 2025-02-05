package ru.t1.java.demo.adminKafka;

import org.apache.kafka.clients.admin.*;
import java.util.Properties;

public class AdminKafkaMonitoring {
    public static void main(String[] args) {
        Properties props = new Properties();
        props.put("bootstrap.servers", "localhost:9092");

        try (AdminClient adminClient = AdminClient.create(props)) {
            // Получение списка топиков
            System.out.println("Получение списка топиков");
            ListTopicsResult topics = adminClient.listTopics();
            topics.names().get().forEach(System.out::println);
            System.out.println("------------------------");

            // Получение описания конкретного топика
            DescribeTopicsResult result = adminClient.describeTopics(topics.names().get());
            result.allTopicNames().get().forEach((topicName, description) -> {
                System.out.println("Получение описания конкретного топика");
                System.out.println("Topic: " + topicName);
                System.out.println("Description: " + description);
                System.out.println("------------------------");
            });
        } catch (Exception ex) {
            System.out.println(ex.getMessage());
        }
    }
}
