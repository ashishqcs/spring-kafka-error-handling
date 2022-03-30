package com.midnight.springkafkaerror.consumer;

import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.support.KafkaHeaders;
import org.springframework.messaging.handler.annotation.Header;
import org.springframework.stereotype.Component;

import java.time.LocalDateTime;

@Slf4j
@Component
public class RetryConsumer {

    @KafkaListener(topics = "products-retry", containerFactory = "kafkaRetryListenerContainerFactory")
    public void listen(ConsumerRecord<String, String> message, @Header(KafkaHeaders.RECEIVED_TOPIC) String topic) {
        log.info("message consumed - key: {} , value: {}, at: {}, offset: {}", message.key(), message.value(), LocalDateTime.now(), message.offset());
        throw new RuntimeException("Exception in retry consumer");
    }

    @KafkaListener(topics = "products-retry.DLT")
    public void listenDLT(String message, @Header(KafkaHeaders.RECEIVED_TOPIC) String topic) {
        log.info("received DLT message : [{}] from topic : [{}] at [{}]", message, topic, LocalDateTime.now());
    }

   /* @DltHandler
    public void topicDLT(String message, @Header(KafkaHeaders.RECEIVED_TOPIC) String topic) {
        log.info("received DLT message : [{}] from topic : [{}]", message, topic);
    }*/

}
