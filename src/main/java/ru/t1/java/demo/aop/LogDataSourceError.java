package ru.t1.java.demo.aop;

import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.producer.*;
import org.aspectj.lang.JoinPoint;
import org.aspectj.lang.annotation.*;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;
import ru.t1.java.demo.buildBeansTopicsKafka.BuildBeansTopicsKafkaConfig;
import ru.t1.java.demo.configuration.kafkaProducer.KafkaCreateProducer;
import ru.t1.java.demo.service.LogErrorDataProcessorService;

@Slf4j
@Aspect
@Component
public class LogDataSourceError {

    private final KafkaCreateProducer kafkaCreateProducer;
    private final BuildBeansTopicsKafkaConfig buildBeansTopicsKafkaConfig;
    private final LogErrorDataProcessorService logErrorDataProcessorService;

    @Autowired
    LogDataSourceError(
        KafkaCreateProducer kafkaCreateProducer,
        BuildBeansTopicsKafkaConfig buildBeansTopicsKafkaConfig,
        LogErrorDataProcessorService logErrorDataProcessorService
    ){
        this.kafkaCreateProducer = kafkaCreateProducer;
        this.buildBeansTopicsKafkaConfig = buildBeansTopicsKafkaConfig;
        this.logErrorDataProcessorService = logErrorDataProcessorService;
    }

    @AfterThrowing(pointcut = "execution(public * ru.t1.java.demo.service.DataProcessorService.*(..))", throwing = "ex")
    public void logAfterThrowing(JoinPoint joinPoint, Throwable ex) {
        NewTopic topicT1DemoMetrics = buildBeansTopicsKafkaConfig.t1_demo_metrics();

        ProducerRecord<String, String> producerRecord = new ProducerRecord<>(topicT1DemoMetrics.name(), "Method Signature", joinPoint.getSignature().toString());
        producerRecord.headers().add("DATA_SOURCE", ex.getMessage().getBytes());

        try{
            // если использовать kafkaProducer с закрытием как try(){} или .close повторные отправки сообщений не попадут в кафка
            KafkaProducer<String, String> kafkaProducer = kafkaCreateProducer.getKafkaProducer();
            // recordMetadata можно использовать для вывода информации вместо metadata, но надо инициализировать recordMetadata
            RecordMetadata recordMetadata = kafkaProducer.send(producerRecord,
                ((metadata, exception) -> {
                    if (exception != null){
                        errorSendToKafka(metadata, joinPoint, ex);
                    } else {
                        messageSentToKafka(metadata);
                    }
                })).get();// синхронная отправка сообщения о подтверждении получения, поток блокируется
        } catch (Exception exception) {
            errorCreateProducer(joinPoint, ex);
        }
    }
    public void errorSendToKafka(RecordMetadata recordMetadata, JoinPoint joinPoint, Throwable ex){
        log.info("===== Начало =====");
        log.info("Ошибка при отправке сообщения в Kafka: {}", recordMetadata.topic());
        log.info("===== Конец  =====");
        logErrorDataProcessorService.logAfterThrowing(joinPoint, ex);
    }
    public void messageSentToKafka(RecordMetadata recordMetadata){
        log.info("==== Начало ====");
        log.info("Сообщение отправлено в Topic: {}", recordMetadata.topic());
        log.info("Сообщение отправлено в Partition: {}", recordMetadata.partition());
        log.info("==== Конец  ====");
    }
    public void errorCreateProducer(JoinPoint joinPoint, Throwable ex){
        log.info("====== Начало ======");
        log.info("Не удалось создать Producer: {}", ex.getMessage());
        log.info("====== Конец ======");
        logErrorDataProcessorService.logAfterThrowing(joinPoint,ex);
    }

}
