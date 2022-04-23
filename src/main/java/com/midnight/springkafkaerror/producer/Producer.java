package com.midnight.springkafkaerror.producer;

import lombok.extern.slf4j.Slf4j;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Component;

import java.util.concurrent.ExecutionException;

@Slf4j
@Component
public class Producer {

    private final KafkaTemplate<String, String> kafkaTemplate;

    public Producer(KafkaTemplate<String, String> kafkaTemplate) {
        this.kafkaTemplate = kafkaTemplate;
    }

    public void send(String topic, String key, String value) throws ExecutionException, InterruptedException {
        log.info("sending key='{}' value='{}' to topic='{}'", key, value, topic);
        kafkaTemplate.send(topic, key, value).get();
    }
}
