package ru.t1.java.demo.aop;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;
import ru.t1.java.demo.buildBeansTopicsKafka.BuildBeansTopicsKafkaConfig;
import ru.t1.java.demo.configuration.kafkaProducer.KafkaCreateProducer;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.producer.*;
import org.aspectj.lang.ProceedingJoinPoint;
import org.aspectj.lang.annotation.*;
import lombok.extern.slf4j.Slf4j;
import java.util.Arrays;

@Slf4j
@Aspect
@Component
public class MetricAspect {

    private final KafkaCreateProducer kafkaCreateProducer;
    private final BuildBeansTopicsKafkaConfig buildBeansTopicsKafkaConfig;
    private Object object;

    @Autowired
    MetricAspect(
        KafkaCreateProducer kafkaCreateProducer,
        BuildBeansTopicsKafkaConfig buildBeansTopicsKafkaConfig
    ){
        this.kafkaCreateProducer = kafkaCreateProducer;
        this.buildBeansTopicsKafkaConfig = buildBeansTopicsKafkaConfig;
    }
    @Around("@annotation(ru.t1.java.demo.aop.AroundDataProcessorService)")
    public Object methodExecutionTime(ProceedingJoinPoint joinPoint){

        NewTopic topicT1DemoMetrics = buildBeansTopicsKafkaConfig.t1_demo_metrics();
        String value = getTimeWorkMethod(joinPoint) + " " + joinPoint.getSignature().toString() + " " + Arrays.toString(joinPoint.getArgs());
        ProducerRecord<String, String> producerRecord = new ProducerRecord<>(topicT1DemoMetrics.name(), "Время работы метода", value);;
        try {
            if (getTimeWorkMethod(joinPoint) > 1) {
                try{
                    sendMassageToKafka(producerRecord);
                } catch (Exception exception) {
                    errorCreateProducer(exception);
                }
            } else if (getTimeWorkMethod(joinPoint) < 2  ){
                throw new RuntimeException("Метод работает очень медленно");
            }
        } catch (Exception ex) {
            producerRecord.headers().add("METRICS", ex.getMessage().getBytes());
            try{
                sendMassageToKafka(producerRecord);
            } catch (Exception exception) {
                errorCreateProducer(exception);
                log.info("ProducerRecord :{}", "null");
            }
        }
        return this.object;
    }
    public void sendMassageToKafka(ProducerRecord<String, String> producerRecord){
        KafkaProducer<String, String> kafkaProducer = kafkaCreateProducer.getKafkaProducer();
        try {
            RecordMetadata recordMetadata = kafkaProducer.send(producerRecord,
                ((metadata, exception) -> {
                    if (exception != null){
                        errorSendToKafka(metadata, exception);
                    } else {
                        messageSentToKafka(metadata);
                    }
                })).get();
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }
    public long getTimeWorkMethod(ProceedingJoinPoint joinPoint){
        long startTime = System.currentTimeMillis();
        try {
            this.object = joinPoint.proceed();
        } catch (Throwable ex) {
            log.info("====== Ошибка proceed() ====== :{}", ex.getMessage().toUpperCase());
            throw new RuntimeException(ex);
        }
        long endTime = System.currentTimeMillis();
        return endTime - startTime;
    }
    public void errorSendToKafka(RecordMetadata recordMetadata, Exception exception){
        log.info("===== Начало =====");
        log.info("Ошибка при отправке сообщения в Kafka: {}", recordMetadata.topic());
        log.info("Ошибка: {}", exception.getMessage().toUpperCase());
        log.info("===== Конец  =====");
    }
    public void messageSentToKafka(RecordMetadata recordMetadata){
        log.info("==== Начало ====");
        log.info("Сообщение отправлено в Topic: {}", recordMetadata.topic());
        log.info("Сообщение отправлено в Partition: {}", recordMetadata.partition());
        log.info("==== Конец  ====");
    }
    public void errorCreateProducer(Exception ex){
        log.info("====== Начало ======");
        log.info("Не удалось создать Producer: {}", ex.getMessage().toUpperCase());
        log.info("====== Конец ======");
    }
}
