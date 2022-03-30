package com.midnight.springkafkaerror.consumer.blockingretry;

import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.support.KafkaHeaders;
import org.springframework.messaging.handler.annotation.Header;
import org.springframework.stereotype.Component;

import java.time.LocalDateTime;

@Slf4j
@Component
public class BlockingRetryConsumer {

    @KafkaListener(topics = "products-retry", containerFactory = "kafkaBlockingRetryContainerFactory")
    public void listen(ConsumerRecord<String, String> message, @Header(KafkaHeaders.RECEIVED_TOPIC) String topic) {
        log.info("retrying message - key: {} , value: {}, at: {}, offset: {}", message.key(), message.value(), LocalDateTime.now(), message.offset());
        throw new RuntimeException("Exception in retry consumer");
    }

    @KafkaListener(topics = "products-retry.DLT")
    public void listenDLT(String message, @Header(KafkaHeaders.RECEIVED_TOPIC) String topic) {
        log.info("received DLT message : [{}] from topic : [{}] at [{}]", message, topic, LocalDateTime.now());
    }

}
